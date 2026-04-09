# requirements: project

from sqlalchemy import Engine, text
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
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name", "desc", "technical_descendants"]
COLLECTION_FIELDS = [
    {"name": "id", "type": "string"},
    {"name": "updated_at", "type": "int64", "sort": True},
    *translated_schema_fields(
        {
            "name": "string",
            "desc": "string",
            "technical_descendants": "string[]",
        }
    ),
]


def _load_material_tree(crdb: Engine) -> pl.DataFrame:
    with crdb.connect() as conn:
        result = conn.execute(
            text("SELECT ancestor_id, descendant_id, depth FROM public.material_tree")
        )
        rows = result.fetchall()
    return pl.DataFrame(
        rows,
        schema=["ancestor_id", "descendant_id", "depth"],
        orient="row",
    )


def index_materials(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the materials in Typesense.

    For incremental updates, we expand the key set to include all ancestors of the
    changed materials so that their technical_descendants lists are recomputed too.
    """
    tree_df = _load_material_tree(crdb)

    # Expand keys to include all ancestors (depth > 0) so ancestor docs are refreshed
    ancestor_rows = tree_df.filter(
        pl.col("descendant_id").is_in(keys) & pl.col("depth").gt(0)
    )
    expanded_ids = list(set(keys) | set(ancestor_rows["ancestor_id"].to_list()))

    df_iter = export_table_by_ids(
        crdb,
        "public.materials",
        ids=expanded_ids,
        cols='id, updated_at, name::string, "desc"::string, technical',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.materials")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        df = df.filter(pl.col("id").ne("MATERIAL_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        for doc in docs:
            if not doc["technical"]:
                tree_df_filtered = tree_df.filter(
                    (pl.col("ancestor_id") == doc["id"]) & (pl.col("depth") > 0)
                )
                tech_desc: list[object] = []
                if tree_df_filtered.height > 0:
                    doc["technical_descendants"] = tech_desc
                for row in tree_df_filtered.iter_rows():
                    descendant_id = str(row[1])
                    for doc2 in docs:
                        if doc2["id"] == descendant_id and doc2["technical"]:
                            tech_desc.append(doc2["name"])
        import_documents(
            ts,
            resolve_collection_name("materials", collection_suffix),
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
            "materials",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_materials(crdb, ts, keys, collection_suffix=collection_suffix)
