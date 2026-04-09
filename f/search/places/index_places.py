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
from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)

LANG_FIELDS = ["name", "desc"]
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "geo", "type": "geopoint", "optional": True},
    {"name": "tags", "type": "string[]", "optional": True, "facet": True},
    *translated_schema_fields(
        {
            "name": "string",
            "desc": "string",
        }
    ),
]


def index_places(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the places in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.places",
        ids=keys,
        cols='id, updated_at, name::string, "desc"::string, st_asgeojson(location) as location',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.places")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        if "name" in df.columns:
            df = df.with_columns(pl.col("name").str.strip_chars())
        if "desc" in df.columns:
            df = df.with_columns(pl.col("desc").str.strip_chars())
        docs = df.to_dicts()
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.places_tags",
            "place_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            if doc.get("location"):
                geojson = json.loads(doc["location"])
                coords = geojson.get("coordinates", [])
                if len(coords) >= 2:
                    doc["geo"] = [coords[1], coords[0]]
            del doc["location"]
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        import_documents(
            ts,
            resolve_collection_name("places", collection_suffix),
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
            "places",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_places(crdb, ts, keys, collection_suffix=collection_suffix)
