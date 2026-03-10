# requirements: project

from sqlalchemy import Engine
import polars as pl
import meilisearch
import json

from f.utils.db.meili import (
    meili_connect,
    check_create_lang_indexes,
    split_docs_by_lang,
    SUPPORTED_LANGS,
)
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name", "desc", "desc_short"]


def index_categories(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the categories in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.categories",
        ids=keys,
        cols='id, name::string, desc_short::string, "desc"::string, image_url',
        schema={"name": pl.String, "desc_short": pl.String, "desc": pl.String},
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.categories")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        df = df.filter(pl.col("id").ne("CATEGORY_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
            doc["desc_short"] = json.loads(str(doc["desc_short"] or "{}"))
        for lang in SUPPORTED_LANGS:
            lang_docs = split_docs_by_lang(docs, LANG_FIELDS, lang)
            _ = meili.index(f"categories_{lang}").add_documents(lang_docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_lang_indexes(
        meili,
        "categories",
        {"searchableAttributes": ["name", "desc_short", "desc"]},
        lang_fields=LANG_FIELDS,
    )
    index_categories(crdb, meili, keys)
