# requirements: project

from sqlalchemy import Engine, insert, text
from sqlalchemy.orm import Session

from f.db.databot.model import OFFProduct
from f.db.sage.model import ExternalSource
from f.graphql.api_client.input_types import (
    CreateOrgInput,
    CreateVariantInput,
    SourceInput,
    UpdateVariantInput,
    VariantOrgsInput,
    VariantRegionsInput,
)
from f.graphql.api_client.client import Client
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.db.meili import MeiliClient, meili_client
from f.utils.general import slugify
from f.utils.log import cfg_log


def resolve_regions(
    meili: MeiliClient,
    countries_tags: list[str],
) -> list[VariantRegionsInput]:
    """Convert OFF countries_tags (e.g. ['en:france']) to CRDB region IDs via Meilisearch."""
    regions = []
    seen_ids: set[str] = set()
    for tag in countries_tags:
        # Strip language prefix (e.g. "en:france" -> "france")
        parts = tag.split(":", 1)
        country_name = parts[-1].replace("-", " ")
        hits = meili.ranking_search(
            "regions_en",
            country_name,
            threshold=0.6,
            limit=1,
            filter="placetype = country",
        )
        hit = hits[0] if hits else None
        if hit:
            region_id = hit["id"]
            if region_id not in seen_ids:
                seen_ids.add(region_id)
                regions.append(VariantRegionsInput(id=region_id))
    return regions


def off_variant(
    product_id: str,
    crdb: Engine | None = None,
    meili: MeiliClient | None = None,
    client: Client | None = None,
):
    """
    Processes an imported OpenFoodFacts product and creates/updates the variant.

    Optional resource params allow callers to supply pre-created clients for
    efficiency (e.g. batch processing). Note: client (httpx-based) is not
    thread-safe — each thread must pass its own client instance.
    """

    if crdb is None:
        crdb = create_sql_engine()
    if meili is None:
        meili = meili_client()
    if client is None:
        client, _ = api_connect()

    # Ensure the OFF source exists
    off_source_id = "g6OJVnSzQkE0mHtYS31O9"
    # Fetch variant tag definitions
    with crdb.begin() as conn:
        tag_defs = (
            conn.execute(
                text(
                    "SELECT id, tag_id, meta_template FROM public.tags WHERE type = 'VARIANT'"
                )
            )
            .scalars()
            .all()
        )
    origins_def = None
    for tag_def in tag_defs:
        if tag_def[1] == "origins":
            origins_def = tag_def[0]
            break

    # Get the variant from the external sources table
    variant_id = None
    with Session(crdb) as session:
        external_sources = (
            session.query(ExternalSource)
            .where(
                ExternalSource.source == "OFF",
                ExternalSource.source_id == product_id.removeprefix("off_"),
            )
            .first()
        )
        if external_sources:
            variant_id = external_sources.variant_id

    # Fetch the OFF product
    with Session(crdb) as session:
        product = session.query(OFFProduct).where(OFFProduct.id == product_id).first()
    if not product:
        print(f"No OFF product found with id {product_id}")
        return
    print("Queried OFF product")

    # Format name translations
    if not product.product_name:
        print(f"No product name found for product {product.id}, skipping")
        return
    name_list = product.product_name.product_name
    print(f"Product: {product.id} {name_list}")
    if not name_list or len(name_list) == 0:
        print(f"No name translations found for product {product.id}, skipping")
        return
    input_names = []
    for name_val in name_list:
        lang = name_val["lang"]
        if lang == "main":
            input_names.append({"lang": "xx", "text": name_val["text"]})
        else:
            input_names.append({"lang": lang, "text": name_val["text"]})
    # Use EAN-13/GTIN code if available
    # OFF uses the 200 prefix to indicate no barcode
    code = None
    if not product.id.removeprefix("off_").startswith("200"):
        code = product.id.removeprefix("off_")

    main_lang = product.lang
    is_en = main_lang == "en"
    tags = []
    # Add origins tag
    origins_tag = {}
    if product.origins:
        origins = product.origins.split(",")
        origin_arr = []
        for origin in origins:
            name = {main_lang: origin}
            if not is_en:
                name["xx"] = origin
            origin_arr.append({"name": name})
        if len(origin_arr) > 0:
            origins_tag["origins"] = origin_arr
    if product.emb_codes:
        emb_codes = product.emb_codes.split(",")
        if len(emb_codes) > 0:
            origins_tag["emb_codes"] = emb_codes
    if product.manufacturing_places:
        name = {main_lang: product.manufacturing_places}
        if not is_en:
            name["xx"] = product.manufacturing_places
        origins_tag["manufacturing"] = [{"name": name}]
    if product.stores:
        stores = product.stores.split(",")
        stores_list = []
        for store in stores:
            name = {main_lang: store}
            if not is_en:
                name["xx"] = store
            stores_list.append({"name": name})
        if len(stores_list) > 0:
            origins_tag["stores"] = stores_list
    if len(origins_tag) > 0 and origins_def:
        tags.append({"id": origins_def, "meta": origins_tag})

    # Find and possibly create orgs
    orgs: list[VariantOrgsInput] = []
    if product.brands:
        brands = [b.strip() for b in product.brands.split(",")]
        slugs = [slugify(b) for b in brands]
        with crdb.begin() as conn:
            rows = conn.execute(
                text("SELECT id, slug FROM public.orgs WHERE slug = ANY(:slugs)"),
                {"slugs": slugs},
            ).fetchall()
        slug_to_id = {row[1]: row[0] for row in rows}
        seen_ids: set[str] = set()
        for brand, slug in zip(brands, slugs):
            if slug in slug_to_id:
                org_id = slug_to_id[slug]
                if org_id not in seen_ids:
                    seen_ids.add(org_id)
                    orgs.append(VariantOrgsInput(id=org_id))
            else:
                # Create a new org
                org = CreateOrgInput(name=brand, slug=slug)
                try:
                    op = client.add_org(org)
                except Exception as e:
                    print(f"Brand search: {brand}")
                    print(f"Failed to create org: {e}")
                    return
                if op.create_org and op.create_org.org:
                    new_id = op.create_org.org.id
                    if new_id not in seen_ids:
                        seen_ids.add(new_id)
                        orgs.append(VariantOrgsInput(id=new_id))

    # Resolve countries_tags to region IDs
    regions: list[VariantRegionsInput] = []
    if product.countries_tags and product.countries_tags.countries_tags:
        regions = resolve_regions(meili, product.countries_tags.countries_tags)

    if variant_id:
        # Update the variant
        input = UpdateVariantInput(id=variant_id)
        input.name_tr = input_names
        input.code = code
        input.add_tags = tags
        input.add_orgs = orgs
        if len(regions) > 0:
            input.region = regions[0]
            if len(regions) > 1:
                input.add_regions = regions[1:]
        op = client.update_variant(input)
        if not op.update_variant or not op.update_variant.variant:
            print(f"Failed to update variant for product {product.id}")
        print(f"Updated variant {variant_id}")
        return
    # Create a new variant
    input = CreateVariantInput()
    input.name_tr = input_names
    input.code = code
    input.tags = tags
    input.add_sources = [SourceInput(id=off_source_id)]
    input.orgs = orgs
    if len(regions) > 0:
        input.region = regions[0]
        if len(regions) > 1:
            input.regions = regions[1:]
    op = client.add_variant(input)
    if not op.create_variant or not op.create_variant.variant:
        print(f"Failed to create variant for product {product.id}")
        return
    print(f"Created variant {op.create_variant.variant.id}")
    with Session(crdb) as sess:
        sess.execute(
            insert(ExternalSource).values(
                source="OFF",
                source_id=product.id.removeprefix("off_"),
                variant_id=op.create_variant.variant.id,
            ),
        )
        sess.commit()


def main(product_id: str):
    cfg_log()
    off_variant(product_id)
