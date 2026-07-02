# requirements: project

import json

import polars as pl
import typesense as typesense_sdk
from sqlalchemy import Engine

from f.search.orgs.rank_orgs import main as rank_main
from f.utils.db.crdb import create_sql_engine, export_table_by_ids
from f.utils.db.typesense import (
    add_mistral_embeddings,
    check_create_aliased_collection,
    expand_translated_docs,
    import_documents,
    mistral_embedding_field,
    resolve_collection_name,
    translated_field_names,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)

LANG_FIELDS = ["desc"]
EMBED_FIELDS = ["name", *translated_field_names(LANG_FIELDS)]


def collection_fields() -> list[dict[str, object]]:
    return [
        {"name": "updated_at", "type": "int64", "sort": True},
        {"name": "rank_order", "type": "float", "sort": True, "optional": True},
        {"name": "name", "type": "string"},
        *translated_schema_fields({"desc": "string"}),
        mistral_embedding_field(EMBED_FIELDS),
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
        cols='id, updated_at, name, "desc"::string'
        + ", (rank->>'order')::FLOAT8 AS rank_order",
        schema={"name": pl.String, "desc": pl.String, "rank_order": pl.Float64},
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.orgs")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        for doc in docs:
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        expanded_docs = expand_translated_docs(docs, LANG_FIELDS)
        import_documents(
            ts,
            resolve_collection_name("orgs", collection_suffix),
            add_mistral_embeddings(expanded_docs, EMBED_FIELDS),
        )


def main(
    keys: list[str],
    check: bool = True,
    collection_suffix: str | None = None,
):
    rank_main(keys)
    crdb = create_sql_engine()
    ts = ts_connect()
    if check:
        check_create_aliased_collection(
            ts,
            "orgs",
            collection_fields(),
            collection_suffix=collection_suffix,
        )
    index_orgs(crdb, ts, keys, collection_suffix=collection_suffix)
