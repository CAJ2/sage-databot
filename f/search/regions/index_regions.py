# requirements: project

from typing import cast
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
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name"]
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "placetype", "type": "string", "facet": True},
    {"name": "admin_level", "type": "int32", "sort": True, "optional": True},
    {"name": "geo", "type": "geopoint", "optional": True},
    *translated_schema_fields({"name": "string"}),
]


def index_regions(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the regions in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.regions",
        ids=keys,
        cols="id, updated_at, name::string, properties::string, placetype, admin_level",
        batch_size=1000,
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.regions")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            prop = cast(dict[str, object], json.loads(str(doc["properties"])))
            doc["geo"] = [prop["geom:latitude"], prop["geom:longitude"]]
            del doc["properties"]
        import_documents(
            ts,
            resolve_collection_name("regions", collection_suffix),
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
            "regions",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_regions(crdb, ts, keys, collection_suffix=collection_suffix)
