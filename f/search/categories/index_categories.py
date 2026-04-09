# requirements: project

from sqlalchemy import Engine
import polars as pl
import json
import typesense as typesense_sdk

from f.utils.db.typesense import (
    check_create_aliased_collection,
    expand_translated_docs,
    import_documents,
    resolve_collection_name,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name", "desc", "desc_short"]
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    *translated_schema_fields(
        {
            "name": "string",
            "desc_short": "string",
            "desc": "string",
        }
    ),
]


def index_categories(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the categories in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.categories",
        ids=keys,
        cols='id, updated_at, name::string, desc_short::string, "desc"::string',
        schema={"name": pl.String, "desc_short": pl.String, "desc": pl.String},
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.categories")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        df = df.filter(pl.col("id").ne("CATEGORY_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
            doc["desc_short"] = json.loads(str(doc["desc_short"] or "{}"))
        import_documents(
            ts,
            resolve_collection_name("categories", collection_suffix),
            expand_translated_docs(docs, LANG_FIELDS),
        )


def main(
    keys: list[str],
    check: bool = True,
    collection_suffix: str | None = None,
):
    crdb = create_sql_engine()
    ts = ts_connect()
    if check:
        check_create_aliased_collection(
            ts,
            "categories",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_categories(crdb, ts, keys, collection_suffix=collection_suffix)
