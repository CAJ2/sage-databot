# requirements: project

from sqlalchemy import Engine
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
from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)

LANG_FIELDS = ["name", "desc"]
COLLECTION_FIELDS = [
    {"name": "id", "type": "string"},
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "tags", "type": "string[]", "optional": True, "facet": True},
    *translated_schema_fields({"name": "string", "desc": "string"}),
]


def index_components(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the components in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.components",
        ids=keys,
        cols='id, updated_at, name::string, "desc"::string',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.components")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.components_tags",
            "component_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        import_documents(
            ts,
            resolve_collection_name("components", collection_suffix),
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
            "components",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_components(crdb, ts, keys, collection_suffix=collection_suffix)
