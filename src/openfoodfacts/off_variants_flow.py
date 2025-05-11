from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
import polars as pl
from prefect_sqlalchemy import SqlAlchemyConnector
import json
import httpx
import time

from src.graphql.api_client.client import Client, CreateVariantInput, UpdateVariantInput
from src.utils.logging.loggers import get_logger
from src.utils.db.crdb import create_polars_uri, db_write_dataframe


@flow
def off_variants_flow():
    """
    Scans the imported OpenFoodFacts dataset for variants
    and updates the variants table.
    """
    log = get_logger()

    # Load the data from the database
    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Load the databot user
    user = crdb.fetch_one(
        "SELECT * FROM public.users WHERE email = 'databot@sageleaf.app'",
    )
    if len(user) == 0:
        raise ValueError("No databot user found in the database.")
    # Create an API client
    api_url = Variable.get("api_url")
    r = httpx.post(
        api_url + "/auth/sign-in/email",
        json={
            "email": user[3],
            "password": Secret.load("databot-password").get(),
        },
    )
    if r.status_code != 200:
        raise ValueError(f"Failed to sign in to the API: {r.status_code} {r.text}")
    httpx_client = httpx.Client(base_url=api_url + "/graphql", cookies=r.cookies)
    client = Client(http_client=httpx_client)
    # Test the API connection
    client.get_root_category()
    # Ensure the OFF source exists
    off_source_id = "g6OJVnSzQkE0mHtYS31O9"
    off_source = crdb.fetch_all(
        "SELECT * FROM public.sources WHERE type = 'API' AND location = 'https://world.openfoodfacts.org'"
    )
    if len(off_source) == 0:
        crdb.execute(
            "INSERT INTO public.sources (id, type, location, user_id) VALUES ("
            f"'{off_source_id}', 'API', 'https://world.openfoodfacts.org', '{user[0]}')"
        )
    # Fetch variant tag definitions
    tag_defs = crdb.fetch_all(
        "SELECT id, tag_id, meta_template FROM public.tags WHERE type = 'VARIANT'",
    )
    origins_def = None
    for tag_def in tag_defs:
        if tag_def[1] == "origins":
            origins_def = tag_def[0]
            break

    ITER_SIZE = 1_000
    cursor: str = "off_"
    while True:
        crdb.close()
        off_cur = crdb.fetch_all(
            f"SELECT * FROM databot.off_products WHERE id > '{cursor}' LIMIT {ITER_SIZE}",
        )
        off_df = pl.from_records(off_cur)
        off_cur = None
        start_cursor = cursor
        cursor = off_df.select(pl.col("id")).tail(1).to_series().item()
        variants_cur = crdb.fetch_all(
            f"""
              SELECT s.source_id, s.variant_id, v.*
              FROM public.external_sources s
              LEFT JOIN public.variants v ON v.id = s.variant_id
              WHERE s.source = 'OFF' AND s.source_id > '{start_cursor.removeprefix("off_")}'
              AND s.source_id <= '{cursor.removeprefix("off_")}'
              ORDER BY s.source_id
            """,
        )
        variants_df = pl.from_records(variants_cur)
        variants_cur = None
        if off_df.is_empty():
            log.info("No more data to process.")
            break

        log.info(
            f"Processing {off_df.height} rows from databot.off_variants and {variants_df.height} rows from public.external_sources"
        )
        for row in off_df.iter_rows(named=True):
            # Get the variant from the external sources table
            variant_id = None
            if len(variants_df.columns) > 0:
                variant = variants_df.filter(
                    pl.col("source_id") == row["id"].removeprefix("off_")
                )
                v_series = variant.select(pl.col("variant_id")).to_series()
                if not v_series.is_empty():
                    variant_id = v_series.item()

            # Format name translations
            name_json = json.loads(row["product_name"])
            name_list: dict = name_json["product_name"]
            log.info(f"Name list: {name_list}")
            if len(name_list) == 0:
                log.warning(f"No name translations found for {row['id']}")
                continue
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
            if not row["id"].removeprefix("off_").startswith("200"):
                code = row["id"].removeprefix("off_")

            main_lang = row["lang"]
            is_en = main_lang == "en"
            tags = []
            # Add origins tag
            origins_tag = {}
            if row["origins"]:
                origins = row["origins"].split(",")
                origin_arr = []
                for origin in origins:
                    name = {main_lang: origin}
                    if not is_en:
                        name["xx"] = origin
                    origin_arr.append({"name": name})
                if len(origin_arr) > 0:
                    origins_tag["origins"] = origin_arr
            if row["emb_codes"]:
                emb_codes = row["emb_codes"].split(",")
                if len(emb_codes) > 0:
                    origins_tag["emb_codes"] = emb_codes
            if row["manufacturing_places"]:
                name = {main_lang: row["manufacturing_places"]}
                if not is_en:
                    name["xx"] = row["manufacturing_places"]
                origins_tag["manufacturing"] = [{"name": name}]
            if row["stores"]:
                stores = row["stores"].split(",")
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

            if variant_id:
                # Update the variant
                input = UpdateVariantInput(id=variant_id)
                input.name_tr = input_names
                input.code = code
                input.add_tags = tags
                op = client.update_variant(input)
                time.sleep(0.1)
                continue
            # Create a new variant
            input = CreateVariantInput()
            input.name_tr = input_names
            input.code = code
            input.tags = tags
            input.add_sources = [off_source_id]
            op = client.add_variant(input)
            crdb.execute(
                f"INSERT INTO public.external_sources (source, source_id, variant_id) VALUES ('OFF', '{row['id'].removeprefix('off_')}', '{op.create_variant.variant.id}')"
            )
            time.sleep(0.1)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    off_variants_flow()
