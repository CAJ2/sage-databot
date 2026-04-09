# requirements: project

import json

import typesense as typesense_sdk
from sqlalchemy import Engine

from f.utils.db.crdb import create_sql_engine, export_table_by_ids
from f.utils.db.typesense import (
    check_create_aliased_collection,
    expand_translated_docs,
    import_documents,
    resolve_collection_name,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)

LANG_FIELDS = ["desc"]
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "name", "type": "string"},
    *translated_schema_fields({"desc": "string"}),
]


def index_orgs(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the orgs in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.orgs",
        ids=keys,
        cols='id, updated_at, name, "desc"::string',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.orgs")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        for doc in docs:
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        import_documents(
            ts,
            resolve_collection_name("orgs", collection_suffix),
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
            "orgs",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_orgs(crdb, ts, keys, collection_suffix=collection_suffix)
