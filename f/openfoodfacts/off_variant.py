# requirements: project

from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from f.db.databot.model import OFFProduct
from f.db.sage.model import ExternalSource
from f.graphql.api_client.input_types import (
    CreateOrgInput,
    CreateVariantInput,
    SourceInput,
    UpdateVariantInput,
    VariantOrgsInput,
)
from f.utils.general import slugify
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.db.meili import meili_client


def off_variant(product_id: str):
    """
    Processes an imported OpenFoodFacts product and creates/updates the variant.
    """

    crdb = create_sql_engine()
    meili = meili_client()

    # Create an API client
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

    # Format name translations
    if not product.product_name:
        print(f"No product name found for product {product.id}, skipping")
        return
    name_list = product.product_name.product_name
    print(f"Product: {product.id} {name_list}")
    if len(name_list) == 0:
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
        brands = product.brands.split(",")
        for brand in brands:
            brand = brand.strip()
            hits = meili.ranking_search("orgs", brand, threshold=0.5, limit=1)
            if len(hits) > 0:
                org = hits[0]
                # Update the org
                print(f"Matching orgs: {hits}")
                # Check if orgs already has this org ID
                if not any(o.id == org["id"] for o in orgs):
                    orgs.append(VariantOrgsInput(id=org["id"]))
            else:
                # Create a new org
                org = CreateOrgInput(name=brand, slug=slugify(brand))
                try:
                    op = client.add_org(org)
                except Exception as e:
                    print(f"Brand search: {brand}")
                    print(f"Failed to create org: {e}")
                    return
                if op.create_org and op.create_org.org:
                    orgs.append(VariantOrgsInput(id=op.create_org.org.id))

    if variant_id:
        # Update the variant
        input = UpdateVariantInput(id=variant_id)
        input.name_tr = input_names
        input.code = code
        input.add_tags = tags
        input.add_orgs = orgs
        op = client.update_variant(input)
        if not op.update_variant or not op.update_variant.variant:
            print(f"Failed to update variant for product {product.id}")
        return
    # Create a new variant
    input = CreateVariantInput()
    input.name_tr = input_names
    input.code = code
    input.tags = tags
    input.add_sources = [SourceInput(id=off_source_id)]
    input.orgs = orgs
    op = client.add_variant(input)
    if not op.create_variant or not op.create_variant.variant:
        print(f"Failed to create variant for product {product.id}")
        return
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
    off_variant(product_id)
