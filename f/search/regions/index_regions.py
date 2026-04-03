# requirements: project

from typing import cast
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

LANG_FIELDS = ["name"]


def index_regions(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the regions in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.regions",
        ids=keys,
        cols="id, name::string, properties::string, placetype, admin_level",
        batch_size=1000,
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.regions")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            prop = cast(dict[str, object], json.loads(str(doc["properties"])))
            doc["properties"] = prop
            doc["_geo"] = {
                "lat": prop["geom:latitude"],
                "lng": prop["geom:longitude"],
            }
        for lang in SUPPORTED_LANGS:
            lang_docs = split_docs_by_lang(
                filter_docs_for_lang(docs, LANG_FIELDS, lang), LANG_FIELDS, lang
            )
            if not lang_docs:
                continue
            _ = meili.index(f"regions_{lang}").add_documents(lang_docs)


def main(keys: list[str], check: bool = True):
    crdb = create_sql_engine()
    meili = meili_connect()
    if check:
        check_create_lang_indexes(
            meili,
            "regions",
            {
                "searchableAttributes": ["name", "properties"],
                "filterableAttributes": ["placetype"],
                "sortableAttributes": ["admin_level"],
            },
            lang_fields=LANG_FIELDS,
        )
    index_regions(crdb, meili, keys)
