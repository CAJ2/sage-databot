# requirements: project

from sqlalchemy import Engine
import polars as pl
import meilisearch
import json

from f.utils.db.meili import meili_connect, check_create_index
from f.utils.db.crdb import create_sql_engine, export_table_by_ids


def index_categories(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the categories in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.categories",
        ids=keys,
        cols='id, name::string, desc_short::string, "desc"::string, image_url',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.categories")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        df = df.filter(pl.col("id").ne("CATEGORY_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            desc_short_json = json.loads(doc["desc_short"] or "{}")
            doc["desc_short"] = desc_short_json
        meili.index("categories").add_documents(docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_index(
        meili,
        "categories",
        {"searchableAttributes": ["name", "desc_short", "desc"]},
    )
    index_categories(crdb, meili, keys)
