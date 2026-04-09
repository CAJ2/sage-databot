# requirements: project

from collections.abc import Mapping, Sequence
from sqlalchemy import Engine, text
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
from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)

LANG_FIELDS = ["name", "desc"]
MAX_CATEGORY_PREFIX_COUNT = 5
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "categories", "type": "string[]", "optional": True, "facet": True},
    {"name": "tags", "type": "string[]", "optional": True, "facet": True},
    *translated_schema_fields({"name": "string", "desc": "string"}),
]


def load_categories_by_item_ids(
    crdb: Engine,
    ids: list[str],
) -> dict[str, list[dict[str, object]]]:
    """Load category ids and translated names for each item id."""
    if not ids:
        return {}

    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ic.item_id, ic.category_id, c.name::string AS name "
                + "FROM public.items_categories ic "
                + "JOIN public.categories c ON c.id = ic.category_id "
                + f"WHERE ic.item_id IN ('{ids_join}') "
                + "ORDER BY ic.item_id, ic.category_id"
            )
        ).fetchall()

    categories_by_item: dict[str, list[dict[str, object]]] = {}
    for item_id, category_id, name in rows:
        key = str(item_id)
        categories_by_item.setdefault(key, []).append(
            {
                "id": str(category_id),
                "name": json.loads(str(name)),
            }
        )
    return categories_by_item


def prepend_category_names(
    desc: Mapping[str, object],
    category_names: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    merged_desc = dict(desc)
    for lang in SUPPORTED_LANGS:
        translated_names: list[str] = []
        seen_names: set[str] = set()
        for category in category_names:
            category_name = category.get("name")
            if not isinstance(category_name, Mapping):
                continue
            name = category_name.get(lang)
            if lang == DEFAULT_LANG and not name:
                name = category_name.get("xx")
            if not isinstance(name, str):
                continue
            stripped_name = name.strip()
            if stripped_name == "" or stripped_name in seen_names:
                continue
            seen_names.add(stripped_name)
            translated_names.append(stripped_name)
            if len(translated_names) >= MAX_CATEGORY_PREFIX_COUNT:
                break

        if not translated_names:
            continue

        prefix = f"Categories:\n{'\n'.join(translated_names)}\n"
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


def index_items(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the items in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.items",
        ids=keys,
        cols='id, updated_at, name::string, "desc"::string',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.items")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        categories_by_id = load_categories_by_item_ids(
            crdb,
            [str(doc["id"]) for doc in docs],
        )
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.items_tags",
            "item_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
            categories = categories_by_id.get(str(doc["id"]), [])
            doc["desc"] = prepend_category_names(
                doc["desc"],
                categories,
            )
            category_ids = [
                category_id
                for category in categories
                if isinstance((category_id := category.get("id")), str)
            ]
            if category_ids:
                doc["categories"] = category_ids
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        import_documents(
            ts,
            resolve_collection_name("items", collection_suffix),
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
            "items",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_items(crdb, ts, keys, collection_suffix=collection_suffix)
