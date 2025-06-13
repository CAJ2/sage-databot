from prefect import flow
from prefect.variables import Variable
from prefect.blocks.system import Secret
import polars as pl
from prefect_sqlalchemy import SqlAlchemyConnector
import json
import time
import meilisearch
from jinja2 import Environment, PackageLoader, select_autoescape
from pydantic_ai import Agent

from src.cli import setup_cli
from src.graphql.api_client.client import CreateItemInput, UpdateVariantInput
from src.graphql.api_client.input_types import VariantItemsInput
from src.utils import llm_agent
from src.utils.api import api_connect
from src.utils.logging.loggers import get_logger
from src.utils.extract import extract_any_json


@flow
def variants_connect_flow(**kwargs):
    """
    Scans the imported variants and
    creates/matches items and components.
    """
    log = get_logger()

    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Connect to Meilisearch
    meili = meilisearch.Client(
        Variable.get("meilisearch", default="http://localhost:7700"),
    )

    jinja = Environment(
        loader=PackageLoader("src.openfoodfacts", package_path="prompts"),
        autoescape=select_autoescape(),
    )
    item_template = jinja.get_template("item.jinja")
    llm = llm_agent()
    agent = Agent(llm)

    client, user = api_connect(crdb)

    ITER_SIZE = 1_000
    cursor: str = ""
    while True:
        crdb.close()
        var_cur = crdb.fetch_all(
            f"""
              SELECT s.source_id, s.variant_id, v.*, p.id AS p_id, p.categories AS p_categories,
              p.packagings AS p_packagings, p.product_quantity AS p_product_quantity,
              p.product_quantity_unit AS p_product_quantity_unit, o.name AS org_name,
              i.name AS item_name
              FROM public.external_sources s
              JOIN public.variants v ON v.id = s.variant_id
              LEFT JOIN databot.off_products p ON p.id = 'off_' || s.source_id
              LEFT JOIN public.variants_orgs vo ON vo.variant_id = s.variant_id
              LEFT JOIN public.orgs o ON o.id = vo.org_id
              LEFT JOIN public.variants_items vi ON vi.variant_id = s.variant_id
              LEFT JOIN public.items i ON i.id = vi.item_id
              WHERE s.source = 'OFF' AND s.source_id > '{cursor}'
              ORDER BY s.source_id LIMIT {ITER_SIZE}
            """,
        )
        var_df = pl.from_records(var_cur)
        if var_df.is_empty():
            log.info("No more data to process.")
            break
        var_cur = None
        cursor = var_df.select(pl.col("source_id")).tail(1).to_series().item()

        log.info(f"Processing {var_df.height} variants")
        for row in var_df.group_by(pl.col("variant_id")).all().iter_rows(named=True):
            if len(row["p_id"]) == 0:
                continue

            prompt = item_template.render(
                {
                    "current_items": list(dict.fromkeys(row["item_name"])),
                    "suggestions": [],
                    "product": row["name"],
                }
            )
            log.info(f"Name: {row['name']}")
            response = agent.run_sync(prompt)
            json_list = extract_any_json(response.content)
            if len(json_list) == 0:
                log.warning(f"No JSON found in response for {row['variant_id']}")
                continue
            log.info(f"Generated items: {json_list}")
            if len(json_list) > 0:
                for item in json_list:
                    if item["name"] == "":
                        continue

                    # Check if the item already exists
                    existing = meili.index("items").search(
                        item["name"], {"limit": 1, "rankingScoreThreshold": 0.5}
                    )
                    if len(existing["hits"]) > 0:
                        log.info(f"Item already exists: {item['name']}")
                        continue
                    # Create the item
                    try:
                        new_item = client.add_item(
                            CreateItemInput(
                                name=item["name"],
                                desc=item["desc"],
                                lang="en",
                            )
                        )
                        if new_item.create_item.item.id:
                            client.update_variant(
                                UpdateVariantInput(
                                    id=row["variant_id"],
                                    add_items=[
                                        VariantItemsInput(
                                            id=new_item.create_item.item.id
                                        )
                                    ],
                                )
                            )
                    except Exception as e:
                        log.error(f"Failed to create item: {e}")
        break


if __name__ == "__main__":
    setup_cli(variants_connect_flow)
