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

LANG_FIELDS = ["name", "desc"]


def index_components(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the components in Meilisearch.
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
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        for lang in SUPPORTED_LANGS:
            lang_docs = split_docs_by_lang(
                filter_docs_for_lang(docs, LANG_FIELDS, lang), LANG_FIELDS, lang
            )
            _ = meili.index(f"components_{lang}").add_documents(lang_docs)


def main(keys: list[str], check: bool = True):
    crdb = create_sql_engine()
    meili = meili_connect()
    if check:
        check_create_lang_indexes(
            meili,
            "components",
            {"searchableAttributes": ["name", "desc"]},
            lang_fields=LANG_FIELDS,
        )
    index_components(crdb, meili, keys)
