# requirements: project

import gzip
import io
import json
import os
import tempfile
import urllib.error
from datetime import datetime, timezone
from typing import Any
from urllib.request import urlopen, urlretrieve

import wmill
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from f.db.databot.model import KGCache, OFFProduct, WikidataCache, ensure_cache_tables
from f.db.sage.model import ExternalSource, Source, SourceContent, VariantSources
from f.graphql.api_client.enums import SourceType
from f.graphql.api_client.input_types import (
    CreateSourceInput,
    LinkSourceInput,
    SourceInput,
    UpdateSourceInput,
    UpdateVariantInput,
)
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.s3 import S3Client


def barcode_to_path(barcode: str) -> str:
    """
    Convert a barcode to OpenFoodFacts directory path.
    Example: "4012359114303" -> "/401/235/911/4303"
    """
    # Remove any prefix
    barcode = barcode.replace("off_", "")

    # Pad with zeros if needed to ensure proper length
    if len(barcode) < 13:
        barcode = barcode.zfill(13)

    # Split into groups: 3, 3, 3, 4
    parts = [
        barcode[0:3],
        barcode[3:6],
        barcode[6:9],
        barcode[9:13],
    ]

    return "/" + "/".join(parts)


def off_image_url(barcode: str, image_id: str, size: str = "full") -> str:
    path = barcode_to_path(barcode)

    # Determine filename based on size
    if size == "full":
        filename = f"{image_id}.jpg"
    else:
        filename = f"{image_id}.{size}.jpg"

    url = (
        f"https://openfoodfacts-images.s3.eu-west-3.amazonaws.com/data{path}/{filename}"
    )
    return url


def download_off_image(url: str) -> tuple[str, str] | None:
    """
    Download an image from OpenFoodFacts S3 bucket.
    Returns (temp_file_path, extension) or None if download fails.
    """
    print(f"Downloading image from: {url}")

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    temp_file.close()

    try:
        urlretrieve(url, temp_file.name)
        print(f"Successfully downloaded to: {temp_file.name}")
        return (temp_file.name, ".jpg")
    except Exception as e:
        print(f"Failed to download image from {url}: {e}")
        raise e


def merge_content(
    existing: SourceContent | None, new: dict[str, Any] | None
) -> dict[str, Any] | None:
    existing_dict = existing.model_dump() if existing else {}
    merged = {
        k: v for k, v in {**existing_dict, **(new or {})}.items() if v is not None
    }
    return merged or None


def download_off_json(url: str) -> bytes | None:
    """
    Download a JSON annotation file from OpenFoodFacts S3 bucket.
    Returns raw gzipped bytes, or None if not available.
    """
    print(f"Downloading JSON annotation from: {url}")
    try:
        with urlopen(url) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"No JSON annotation found at {url} (404)")
            return None
        print(f"HTTP error downloading JSON from {url}: {e}")
        return None
    except Exception as e:
        print(f"Failed to download JSON from {url}: {e}")
        return None


def fetch_kg_entities(
    mids: list[str], crdb: Engine, api_key: str
) -> dict[str, dict[str, Any] | None]:
    """
    Fetch Google Knowledge Graph entities for the given MIDs, using CRDB cache.
    Returns {mid: jsonld_or_None} — None means KG returned no result for that MID.
    """
    results: dict[str, dict[str, Any] | None] = {}
    uncached_mids: list[str] = []

    with Session(crdb) as session:
        for mid in mids:
            cached = session.get(KGCache, mid)
            if cached is not None:
                print(f"KG cache hit for {mid}")
                results[mid] = cached.jsonld
            else:
                uncached_mids.append(mid)

    if not uncached_mids:
        return results

    # Batch fetch from KG API (up to 50 MIDs per request)
    BATCH_SIZE = 50
    fetched: dict[str, dict[str, Any] | None] = {}

    for i in range(0, len(uncached_mids), BATCH_SIZE):
        batch = uncached_mids[i : i + BATCH_SIZE]
        params = "&".join(f"ids={mid}" for mid in batch)
        url = f"https://kgsearch.googleapis.com/v1/entities:search?{params}&key={api_key}&indent=False"

        try:
            with urlopen(url) as response:
                data = json.loads(response.read())
        except Exception as e:
            print(f"Failed to fetch KG entities for batch {batch}: {e}")
            for mid in batch:
                fetched[mid] = None
            continue

        # Index results by MID (KG returns @id as "kg:/m/xxxx")
        by_mid: dict[str, dict[str, Any]] = {}
        for item in data.get("itemListElement", []):
            result = item.get("result", {})
            entity_id = result.get("@id", "")
            mid_key = entity_id.replace("kg:", "")
            by_mid[mid_key] = result

        for mid in batch:
            fetched[mid] = by_mid.get(mid)  # None if KG has no result

    # Upsert fetched results into kg_cache
    now = datetime.now(timezone.utc)
    with Session(crdb) as session:
        for mid, jsonld in fetched.items():
            entry = session.get(KGCache, mid)
            if entry is None:
                session.add(KGCache(mid=mid, jsonld=jsonld, fetched_at=now))
            else:
                entry.jsonld = jsonld
                entry.fetched_at = now
        session.commit()

    results.update(fetched)
    return results


def fetch_wikidata_entities(
    wiki_urls: list[str], crdb: Engine
) -> dict[str, dict[str, Any] | None]:
    """
    Fetch minimal Wikidata JSON-LD for Wikipedia URLs extracted from KG results.
    Returns {wiki_url: jsonld_or_None}.

    Steps per URL:
      1. Parse lang + title from URL (e.g. "http://en.wikipedia.org/wiki/Sainsbury%27s")
      2. Query Wikidata wbgetentities API (sitelinks + labels + descriptions) to resolve
         to a QID and collect basic entity metadata
      3. Check wikidata_cache; build and cache a minimal JSON-LD if uncached
    """
    from urllib.parse import quote, unquote, urlparse
    from urllib.request import Request

    WIKIDATA_UA = "databot/1.0 (https://github.com/sageleaf) off_download_images"

    results: dict[str, dict[str, Any] | None] = {}

    # Parse each URL into (lang, title), skip unparseable ones
    url_to_lang_title: dict[str, tuple[str, str]] = {}
    for wiki_url in wiki_urls:
        try:
            parsed = urlparse(wiki_url)
            # hostname: en.wikipedia.org -> lang = "en"
            host_parts = parsed.hostname.split(".") if parsed.hostname else []
            if len(host_parts) < 2 or host_parts[1] != "wikipedia":
                print(f"Skipping non-Wikipedia URL: {wiki_url}")
                results[wiki_url] = None
                continue
            lang = host_parts[0]
            # path: /wiki/Sainsbury's -> title = "Sainsbury's" (unquote percent-encoding)
            path_parts = parsed.path.split("/wiki/", 1)
            if len(path_parts) < 2 or not path_parts[1]:
                print(f"Could not extract title from Wikipedia URL: {wiki_url}")
                results[wiki_url] = None
                continue
            title = unquote(path_parts[1])
            url_to_lang_title[wiki_url] = (lang, title)
        except Exception as e:
            print(f"Failed to parse Wikipedia URL {wiki_url}: {e}")
            results[wiki_url] = None

    if not url_to_lang_title:
        return results

    # Group by language for batch API calls
    lang_to_urls: dict[str, list[str]] = {}
    for wiki_url, (lang, _) in url_to_lang_title.items():
        lang_to_urls.setdefault(lang, []).append(wiki_url)

    # Resolve Wikipedia titles -> QIDs via Wikidata wbgetentities
    url_to_qid: dict[str, str | None] = {}
    qid_to_entity: dict[str, dict[str, Any]] = {}
    for lang, urls in lang_to_urls.items():
        titles = [url_to_lang_title[u][1] for u in urls]
        # URL-encode each title individually; join with | (Wikidata multi-value separator)
        encoded_titles = "|".join(quote(t, safe="") for t in titles)
        api_url = (
            f"https://www.wikidata.org/w/api.php"
            f"?action=wbgetentities&sites={lang}wiki"
            f"&titles={encoded_titles}"
            f"&props=sitelinks|labels|descriptions&languages=en&format=json"
        )
        try:
            req = Request(api_url, headers={"User-Agent": WIKIDATA_UA})
            with urlopen(req) as response:
                data = json.loads(response.read())
        except Exception as e:
            print(f"Failed to query Wikidata wbgetentities for lang={lang}: {e}")
            for u in urls:
                url_to_qid[u] = None
            continue

        # Build title -> QID map from sitelinks in response; capture entity metadata
        title_to_qid: dict[str, str] = {}
        for entity_id, entity in data.get("entities", {}).items():
            if entity_id.startswith("Q"):
                site_link = entity.get("sitelinks", {}).get(f"{lang}wiki", {})
                wiki_title = site_link.get("title", "")
                if wiki_title:
                    title_to_qid[wiki_title] = entity_id
                    qid_to_entity[entity_id] = entity

        for wiki_url in urls:
            _, title = url_to_lang_title[wiki_url]
            url_to_qid[wiki_url] = title_to_qid.get(title)
            if url_to_qid[wiki_url] is None:
                print(
                    f"No Wikidata QID found for Wikipedia title '{title}' (lang={lang})"
                )

    # Check cache for known QIDs; collect uncached ones
    qid_to_jsonld: dict[str, dict[str, Any] | None] = {}
    uncached_qids: list[str] = []

    with Session(crdb) as session:
        for qid in set(q for q in url_to_qid.values() if q):
            cached = session.get(WikidataCache, qid)
            if cached is not None:
                print(f"Wikidata cache hit for {qid}")
                qid_to_jsonld[qid] = cached.jsonld
            else:
                uncached_qids.append(qid)

    # Build minimal JSON-LD for uncached QIDs from entity data already in memory
    for qid in uncached_qids:
        entity = qid_to_entity.get(qid)
        if entity is None:
            qid_to_jsonld[qid] = None
            continue
        labels = entity.get("labels", {})
        descriptions = entity.get("descriptions", {})
        name = (labels.get("en") or next(iter(labels.values()), {})).get("value", "")
        description = (
            descriptions.get("en") or next(iter(descriptions.values()), {})
        ).get("value", "")
        minimal: dict[str, Any] = {
            "@context": {"@vocab": "http://schema.org/"},
            "@id": f"http://www.wikidata.org/entity/{qid}",
            "identifier": qid,
        }
        if name:
            minimal["name"] = name
        if description:
            minimal["description"] = description
        qid_to_jsonld[qid] = minimal
        print(f"Built minimal Wikidata JSON-LD for {qid} ({name})")

    # Upsert into wikidata_cache
    now = datetime.now(timezone.utc)
    with Session(crdb) as session:
        for qid, jsonld in qid_to_jsonld.items():
            if qid in uncached_qids:
                entry = session.get(WikidataCache, qid)
                if entry is None:
                    session.add(WikidataCache(qid=qid, jsonld=jsonld, fetched_at=now))
                else:
                    entry.jsonld = jsonld
                    entry.fetched_at = now
        session.commit()

    # Build final result: wiki_url -> jsonld
    for wiki_url in wiki_urls:
        if wiki_url in results:
            continue  # already set (parse error)
        qid = url_to_qid.get(wiki_url)
        if qid is None:
            results[wiki_url] = None
        else:
            results[wiki_url] = qid_to_jsonld.get(qid)

    return results


def main(
    variant_id: str,
    image_sizes: list[str] | None = None,
):
    """
    Download OpenFoodFacts product images and upload them to S3.

    Args:
        variant_id: The variant ID to process
        image_sizes: List of image sizes to download (e.g., ["full", "400"])
    """

    if image_sizes is None:
        image_sizes = ["full"]
    crdb = create_sql_engine()
    ensure_cache_tables(crdb)
    client, _ = api_connect()
    kg_api_key = wmill.get_variable("f/api_config/gcp_kg_api_key")

    # Initialize S3 client for sources bucket
    s3_client = S3Client(resource_id="f/s3_config/s3_sources")

    # Step 1: Validate variant exists in CRDB
    with Session(crdb) as session:
        stmt = (
            select(VariantSources)
            .join(Source, Source.id == VariantSources.source_id)
            .where(VariantSources.variant_id == variant_id)
        )
        variants = session.scalars(stmt).unique().all()
        if not variants:
            raise ValueError(f"Variant {variant_id} not found in CRDB")

    print(f"Variant {variant_id} found")

    # Step 2: Get OFF external source connection
    with Session(crdb) as session:
        stmt = select(ExternalSource.source_id).where(
            ExternalSource.source == "OFF", ExternalSource.variant_id == variant_id
        )
        external_source = session.scalars(stmt).first()
        if not external_source:
            raise ValueError(
                f"No OpenFoodFacts connection found for variant {variant_id} in external_sources"
            )

        off_barcode = external_source

    print(f"Found OFF barcode: {off_barcode}")

    # Step 3: Get OFF product data
    off_product_id = f"off_{off_barcode}"
    print(f"Fetching OFF product data for {off_product_id}...")

    with Session(crdb) as session:
        stmt = select(OFFProduct).where(OFFProduct.id == off_product_id)
        product = session.scalars(stmt).first()

    if not product:
        raise ValueError(f"No OFF product found with id {off_product_id}")

    if not product.images or not product.images.images:
        raise ValueError(f"No images found for product {off_product_id}")

    print(f"Found product with images: {product.images.images}")

    # Step 4: Extract image IDs from images field
    images_data = product.images.images
    image_ids: dict[str, list[str]] = {}
    # Only get the primary images, not cropped sections
    # Some images have an imgid referring to the larger image it is cropped from
    for image in images_data:
        if image.imgid is None and image.sizes is not None:
            image_ids[image.key] = [k for k in image.sizes.keys() if k in image_sizes]

    if not image_ids:
        raise ValueError(f"No valid image IDs found in product {off_product_id}")

    print(f"Found {len(image_ids)} images: {image_ids}")

    # Step 5: Process each image and size combination
    created_sources = []

    barcode_path = barcode_to_path(off_barcode)

    for image_id, sizes in image_ids.items():
        for size in sizes:
            print(f"\nProcessing image {image_id}, size: {size}")

            url = off_image_url(off_barcode, image_id, size)
            size_suffix = "" if size == "full" else f".{size}"
            s3_key = f"off/{off_barcode}/{image_id}{size_suffix}.jpg"
            public_url = (
                f"https://{s3_client.bucket}.fra1.cdn.digitaloceanspaces.com/{s3_key}"
            )

            # Check if a source with this contentURL is already linked to the variant
            existing_source = next(
                (
                    v.source
                    for v in variants
                    if v.source.type == SourceType.IMAGE and v.source.content_url == url
                ),
                None,
            )

            # Download JSON annotation to determine desired content
            json_url = f"https://openfoodfacts-images.s3.eu-west-3.amazonaws.com/data{barcode_path}/{image_id}.json.gz"
            json_bytes = download_off_json(json_url)
            content = None
            image_mids: list[str] = []
            if json_bytes:
                try:
                    parsed = json.loads(gzip.decompress(json_bytes))
                    response_data = parsed["responses"][0]
                    try:
                        text = response_data["fullTextAnnotation"]["text"]
                        content = {"context": text}
                        print(f"Extracted OCR text ({len(text)} chars)")
                    except (KeyError, TypeError):
                        pass
                    for annotation in response_data.get("logoAnnotations", []):
                        mid = annotation.get("mid")
                        if mid:
                            image_mids.append(mid)
                    for annotation in response_data.get("labelAnnotations", []):
                        mid = annotation.get("mid")
                        if mid:
                            image_mids.append(mid)
                    image_mids = list(dict.fromkeys(image_mids))  # deduplicate
                    if image_mids:
                        print(f"Found {len(image_mids)} KG entity MIDs")
                except Exception as e:
                    print(f"Could not parse JSON annotation: {e}")

            source_id = None

            if existing_source:
                source_id = existing_source.id
                # Check if any fields differ and update if so
                merged_content = merge_content(existing_source.content, content)
                changes: dict[str, Any] = {}
                if existing_source.location != public_url:
                    changes["location"] = public_url
                if merge_content(existing_source.content, None) != merged_content:
                    changes["content"] = merged_content
                if changes:
                    print(
                        f"Updating source {source_id} (changed: {list(changes.keys())})"
                    )
                    client.update_source(UpdateSourceInput(id=source_id, **changes))
                else:
                    print(f"Source {source_id} already up to date")
            else:
                # Download and upload image to S3
                download_result = download_off_image(url)
                if not download_result:
                    print(f"Skipping image {image_id} size {size} - download failed")
                    continue

                temp_path, _ = download_result
                try:
                    s3_url = f"s3://{s3_client.bucket}/{s3_key}"
                    print(f"Uploading to S3: {s3_url}")
                    with open(temp_path, "rb") as f:
                        s3_client.s3_upload(s3_url, f)

                    if json_bytes:
                        json_s3_key = f"off/{off_barcode}/{image_id}.json.gz"
                        json_s3_url = f"s3://{s3_client.bucket}/{json_s3_key}"
                        print(f"Uploading JSON annotation to S3: {json_s3_url}")
                        s3_client.s3_upload(json_s3_url, io.BytesIO(json_bytes))

                    print(f"Public URL: {public_url}")

                    source_input = CreateSourceInput(
                        type=SourceType.IMAGE,
                        location=public_url,
                        contentURL=url,
                        content=content,
                        metadata={
                            "parent_source": "g6OJVnSzQkE0mHtYS31O9",  # OFF source ID in CRDB
                            "key": image_id,
                            "size": size,
                        },
                    )

                    print("Creating source record...")
                    op = client.add_source(source_input)
                    if not op.create_source or not op.create_source.source:
                        print(
                            f"Failed to create source for image {image_id} size {size}"
                        )
                        continue

                    source_id = op.create_source.source.id
                    print(f"Created source: {source_id}")

                finally:
                    try:
                        os.unlink(temp_path)
                    except Exception as e:
                        print(f"Warning: Failed to delete temp file {temp_path}: {e}")

            if source_id is None:  # pyright: ignore[reportUnnecessaryComparison]
                continue

            # Link KG entities to this source
            if image_mids:
                kg_entities = fetch_kg_entities(image_mids, crdb, kg_api_key)
                for mid, jsonld in kg_entities.items():
                    print(jsonld)
                    if jsonld is None:
                        print(f"No KG entity found for MID {mid}, skipping")
                        continue
                    try:
                        client.link_source(LinkSourceInput(id=source_id, jsonld=jsonld))
                        print(f"Linked KG entity {mid} to source {source_id}")
                    except Exception as e:
                        print(
                            f"Failed to link KG entity {mid} to source {source_id}: {e}"
                        )

                # Collect Wikipedia URLs from KG results for Wikidata lookup
                wiki_urls = []
                for jsonld in kg_entities.values():
                    if jsonld is None:
                        continue
                    wiki_url = (jsonld.get("detailedDescription", {}) or {}).get("url")
                    if wiki_url and "wikipedia.org/wiki/" in wiki_url:
                        wiki_urls.append(wiki_url)
                wiki_urls = list(dict.fromkeys(wiki_urls))  # deduplicate

                if wiki_urls:
                    wikidata_entities = fetch_wikidata_entities(wiki_urls, crdb)
                    for wiki_url, wd_jsonld in wikidata_entities.items():
                        print(wd_jsonld)
                        if wd_jsonld is None:
                            print(f"No Wikidata entity found for {wiki_url}, skipping")
                            continue
                        try:
                            client.link_source(
                                LinkSourceInput(id=source_id, jsonld=wd_jsonld)
                            )
                            print(
                                f"Linked Wikidata entity for {wiki_url} to source {source_id}"
                            )
                        except Exception as e:
                            print(
                                f"Failed to link Wikidata entity for {wiki_url} to source {source_id}: {e}"
                            )

            # Link to variant only if not already linked
            if existing_source is None:
                print("Linking source to variant...")
                variant_input = UpdateVariantInput(id=variant_id)
                variant_input.add_sources = [SourceInput(id=source_id)]
                op = client.update_variant(variant_input)
                if not op.update_variant or not op.update_variant.variant:
                    print(
                        f"Failed to link source to variant for image {image_id} size {size}"
                    )
                    continue

            created_sources.append(
                {
                    "variant_id": variant_id,
                    "off_barcode": off_barcode,
                    "source_id": source_id,
                    "image_id": image_id,
                    "size": size,
                    "location": public_url,
                }
            )

    print(f"Successfully created {len(created_sources)} source(s)")

    return created_sources
