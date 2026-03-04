# requirements: project

from sqlalchemy import Engine
import polars as pl
import meilisearch
import json

from f.utils.db.meili import meili_connect, check_create_index
from f.utils.db.crdb import create_sql_engine, export_table_by_ids


def index_orgs(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the orgs in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.orgs",
        ids=keys,
        cols='id, updated_at, name, "desc"::string, avatar_url',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.orgs")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
        meili.index("orgs").add_documents(docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_index(
        meili,
        "orgs",
        {"searchableAttributes": ["name", "desc"]},
    )
    index_orgs(crdb, meili, keys)
