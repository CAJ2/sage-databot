# requirements: project

from collections.abc import Mapping, Sequence
from sqlalchemy import Engine, text
import polars as pl
import json
import typesense as typesense_sdk

from f.utils.db.typesense import (
    DEFAULT_LANG,
    SUPPORTED_LANGS,
    check_create_aliased_collection,
    expand_translated_docs,
    import_documents,
    resolve_collection_name,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)
from f.utils.db.crdb import create_sql_engine, export_table_by_ids

LANG_FIELDS = ["name", "desc"]
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "technical", "type": "bool"},
    {"name": "shape", "type": "string", "optional": True},
    {"name": "ancestors", "type": "string[]", "optional": True},
    {"name": "technical_descendants", "type": "string[]", "optional": True},
    *translated_schema_fields(
        {
            "name": "string",
            "desc": "string",
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


def prepend_ancestor_names(
    desc: Mapping[str, object],
    ancestors: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    merged_desc = dict(desc)
    for lang in SUPPORTED_LANGS:
        translated_names: list[str] = []
        seen_names: set[str] = set()
        for ancestor in ancestors:
            ancestor_name = ancestor.get("name")
            if not isinstance(ancestor_name, Mapping):
                continue
            name = ancestor_name.get(lang)
            if lang == DEFAULT_LANG and not name:
                name = ancestor_name.get("xx")
            if not isinstance(name, str):
                continue
            stripped_name = name.strip()
            if stripped_name == "" or stripped_name in seen_names:
                continue
            seen_names.add(stripped_name)
            translated_names.append(stripped_name)

        if not translated_names:
            continue

        prefix = f"Ancestors:\n{'\n'.join(translated_names)}\n"
        current = merged_desc.get(lang, "")
        if lang == DEFAULT_LANG and (
            not isinstance(current, str) or current.strip() == ""
        ):
            current = merged_desc.get("xx", "")
        if not isinstance(current, str):
            current = ""
        current_text = current.strip()
        merged_desc[lang] = f"{prefix}{current_text}" if current_text else prefix
    return merged_desc


def index_materials(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the materials in Typesense.

    For incremental updates, we expand the key set to include both ancestors and
    descendants of changed materials so ancestor-name prefixes and descendant
    filters stay current.
    """
    tree_df = _load_material_tree(crdb)

    ancestor_rows = tree_df.filter(
        pl.col("descendant_id").is_in(keys) & pl.col("depth").gt(0)
    )
    descendant_rows = tree_df.filter(
        pl.col("ancestor_id").is_in(keys) & pl.col("depth").gt(0)
    )
    expanded_ids = list(
        set(keys)
        | set(ancestor_rows["ancestor_id"].to_list())
        | set(descendant_rows["descendant_id"].to_list())
    )

    df_iter = export_table_by_ids(
        crdb,
        "public.materials",
        ids=expanded_ids,
        cols='id, updated_at, name::string, "desc"::string, technical, shape',
        schema={"name": pl.String, "desc": pl.String, "shape": pl.String},
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.materials")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        df = df.filter(pl.col("id").ne("MATERIAL_ROOT"))
        docs = df.to_dicts()
        docs_by_id: dict[str, dict[str, object]] = {}
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
            docs_by_id[str(doc["id"])] = doc
        for doc in docs:
            material_id = str(doc["id"])
            ancestor_rows_for_doc = tree_df.filter(
                (pl.col("descendant_id") == material_id) & (pl.col("depth") > 0)
            ).sort(["depth", "ancestor_id"])
            ancestors: list[dict[str, object]] = []
            ancestor_ids: list[str] = []
            for ancestor_id, _, _depth in ancestor_rows_for_doc.iter_rows():
                ancestor_key = str(ancestor_id)
                ancestor_doc = docs_by_id.get(ancestor_key)
                if ancestor_doc is None:
                    continue
                ancestor_ids.append(ancestor_key)
                ancestors.append(
                    {
                        "id": ancestor_key,
                        "name": ancestor_doc["name"],
                    }
                )
            if ancestor_ids:
                doc["ancestors"] = ancestor_ids
                doc["desc"] = prepend_ancestor_names(doc["desc"], ancestors)

            descendant_rows_for_doc = tree_df.filter(
                (pl.col("ancestor_id") == material_id) & (pl.col("depth") > 0)
            ).sort(["depth", "descendant_id"])
            technical_descendant_ids: list[str] = []
            for (
                _ancestor_id,
                descendant_id,
                _depth,
            ) in descendant_rows_for_doc.iter_rows():
                descendant_key = str(descendant_id)
                descendant_doc = docs_by_id.get(descendant_key)
                if descendant_doc is None or not bool(descendant_doc["technical"]):
                    continue
                technical_descendant_ids.append(descendant_key)
            if technical_descendant_ids:
                doc["technical_descendants"] = technical_descendant_ids
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
