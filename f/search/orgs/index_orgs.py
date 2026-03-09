# requirements: project

import json

import meilisearch
import polars as pl
from sqlalchemy import Engine

from f.utils.db.crdb import create_sql_engine, export_table_by_ids
from f.utils.db.meili import (
    check_create_lang_indexes,
    meili_connect,
    split_docs_by_lang,
    SUPPORTED_LANGS,
)

LANG_FIELDS = ["desc"]


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
        cols='id, updated_at, name, slug, "desc"::string, avatar_url',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.orgs")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        for lang in SUPPORTED_LANGS:
            lang_docs = split_docs_by_lang(docs, LANG_FIELDS, lang)
            _ = meili.index(f"orgs_{lang}").add_documents(lang_docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_lang_indexes(
        meili,
        "orgs",
        {"searchableAttributes": ["name", "desc"]},
        lang_fields=LANG_FIELDS,
    )
    index_orgs(crdb, meili, keys)
