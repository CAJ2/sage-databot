# requirements: project

import gzip
import io
import json
import os
import tempfile
import urllib.error
from urllib.request import urlopen, urlretrieve

from sqlalchemy import select
from sqlalchemy.orm import Session

from f.db.databot.model import OFFProduct
from f.db.sage.model import ExternalSource, Source, SourceContent, VariantSources
from f.graphql.api_client.client import CreateSourceInput
from f.graphql.api_client.enums import SourceType
from f.graphql.api_client.input_types import (
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


def merge_content(existing: SourceContent | None, new: dict | None) -> dict | None:
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


def main(
    variant_id: str,
    image_sizes: list[str] = ["full"],
):
    """
    Download OpenFoodFacts product images and upload them to S3.

    Args:
        variant_id: The variant ID to process
        image_sizes: List of image sizes to download (e.g., ["full", "400"])
    """

    crdb = create_sql_engine()
    client, user = api_connect()

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
    if isinstance(images_data, list):
        image_ids: dict[str, list[str]] = {}
        # Only get the primary images, not cropped sections
        # Some images have an imgid referring to the larger image it is cropped from
        for image in images_data:
            if image.imgid is None and image.sizes is not None:
                image_ids[image.key] = [
                    k for k in image.sizes.keys() if k in image_sizes
                ]
    else:
        raise ValueError(f"Unexpected images data format: {type(images_data)}")

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
            if json_bytes:
                try:
                    parsed = json.loads(gzip.decompress(json_bytes))
                    text = parsed["responses"][0]["fullTextAnnotation"]["text"]
                    content = {"context": text}
                    print(f"Extracted OCR text ({len(text)} chars)")
                except Exception as e:
                    print(f"Could not extract OCR text from JSON: {e}")

            source_id = None

            if existing_source:
                source_id = existing_source.id
                # Check if any fields differ and update if so
                merged_content = merge_content(existing_source.content, content)
                changes: dict = {}
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

            if source_id is None:
                continue

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
