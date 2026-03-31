# requirements: project

from sqlalchemy import Engine
import polars as pl
import meilisearch
import json

from f.utils.db.meili import (
    meili_connect,
    check_create_lang_indexes,
    filter_docs_for_lang,
    split_docs_by_lang,
    SUPPORTED_LANGS,
)
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name", "address", "desc"]


def index_places(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the places in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.places",
        ids=keys,
        cols='id, updated_at, name::string, address::string, "desc"::string, st_asgeojson(location) as location',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.places")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        if "name" in df.columns:
            df = df.with_columns(pl.col("name").str.strip_chars())
        if "address" in df.columns:
            df = df.with_columns(pl.col("address").str.strip_chars())
        if "desc" in df.columns:
            df = df.with_columns(pl.col("desc").str.strip_chars())
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            address_json = json.loads(doc["address"] or "{}")
            doc["address"] = address_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            if doc.get("location"):
                geojson = json.loads(doc["location"])
                coords = geojson.get("coordinates", [])
                if len(coords) >= 2:
                    doc["_geo"] = {"lat": coords[1], "lng": coords[0]}
            del doc["location"]
        for lang in SUPPORTED_LANGS:
            lang_docs = split_docs_by_lang(
                filter_docs_for_lang(docs, LANG_FIELDS, lang), LANG_FIELDS, lang
            )
            _ = meili.index(f"places_{lang}").add_documents(lang_docs)


def main(keys: list[str], check: bool = True):
    crdb = create_sql_engine()
    meili = meili_connect()
    if check:
        check_create_lang_indexes(
            meili,
            "places",
            {
                "searchableAttributes": ["name", "address", "desc"],
                "filterableAttributes": ["_geo"],
            },
            lang_fields=LANG_FIELDS,
        )
    index_places(crdb, meili, keys)
