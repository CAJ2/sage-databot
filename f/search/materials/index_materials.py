# requirements: project

from sqlalchemy import Engine, text
import polars as pl
import meilisearch
import json

from f.utils.db.meili import meili_connect, check_create_index
from f.utils.db.crdb import create_sql_engine, export_table_by_ids


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
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the materials in Meilisearch.

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
        cols='id, name::string, "desc"::string, technical',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.materials")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        df = df.filter(pl.col("id").ne("MATERIAL_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
        for doc in docs:
            if not doc["technical"]:
                tree_df_filtered = tree_df.filter(
                    (pl.col("ancestor_id") == doc["id"]) & (pl.col("depth") > 0)
                )
                if tree_df_filtered.height > 0:
                    doc["technical_descendants"] = []
                for row in tree_df_filtered.iter_rows():
                    descendant_id = row[1]
                    for doc2 in docs:
                        if doc2["id"] == descendant_id and doc2["technical"]:
                            doc["technical_descendants"].append(doc2["name"])
        meili.index("materials").add_documents(docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_index(
        meili,
        "materials",
        {"searchableAttributes": ["name", "desc", "technical_descendants"]},
    )
    index_materials(crdb, meili, keys)
