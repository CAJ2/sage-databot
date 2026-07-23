# requirements: project

import json

import polars as pl
import typesense as typesense_sdk
from sqlalchemy import Engine

from f.search.programs.rank_programs import main as rank_main
from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)
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

LANG_FIELDS = ["name", "desc"]
EMBED_FIELDS = translated_field_names(LANG_FIELDS)


def collection_fields() -> list[dict[str, object]]:
    return [
        {"name": "updated_at", "type": "int64", "sort": True},
        {"name": "rank_order", "type": "float", "sort": True, "optional": True},
        {"name": "tags", "type": "string[]", "optional": True, "facet": True},
        {"name": "status", "type": "string", "optional": True},
        *translated_schema_fields(
            {
                "name": "string",
                "desc": "string",
            }
        ),
        mistral_embedding_field(EMBED_FIELDS),
    ]


def index_programs(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    df_iter = export_table_by_ids(
        crdb,
        "public.programs",
        ids=keys,
        cols='id, updated_at, name::string, "desc"::string, status'
        + ", (rank->>'order')::FLOAT8 AS rank_order",
        schema={
            "name": pl.String,
            "desc": pl.String,
            "status": pl.String,
            "rank_order": pl.Float64,
        },
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.programs")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        if "name" in df.columns:
            df = df.with_columns(pl.col("name").str.strip_chars())
        if "desc" in df.columns:
            df = df.with_columns(pl.col("desc").str.strip_chars())
        docs = df.to_dicts()
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.program_tags",
            "program_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        expanded_docs = expand_translated_docs(docs, LANG_FIELDS)
        import_documents(
            ts,
            resolve_collection_name("programs", collection_suffix),
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
            "programs",
            collection_fields(),
            collection_suffix=collection_suffix,
        )
    index_programs(crdb, ts, keys, collection_suffix=collection_suffix)
