# requirements: project

from collections.abc import Mapping, Sequence
from sqlalchemy import Engine
from sqlalchemy import text
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
STANDARD_BARCODE_LENGTHS = [7, 8, 12, 13, 14]
MAX_ITEM_DESC_COUNT = 5
COLLECTION_FIELDS = [
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "code", "type": "string[]", "optional": True},
    {"name": "items", "type": "string[]", "optional": True, "facet": True},
    {"name": "tags", "type": "string[]", "optional": True, "facet": True},
    *translated_schema_fields({"name": "string", "desc": "string"}),
]


def barcode_forms(code: object) -> list[str] | None:
    if code is None:
        return None

    value = str(code).strip()
    if value == "":
        return None
    if not value.isdigit():
        return [value]

    canonical = value.lstrip("0") or "0"
    forms: list[str] = []
    for candidate in [value, canonical]:
        if candidate not in forms:
            forms.append(candidate)

    for length in STANDARD_BARCODE_LENGTHS:
        if len(canonical) <= length:
            padded = canonical.zfill(length)
            if padded not in forms:
                forms.append(padded)

    return forms


def load_items_by_variant_ids(
    crdb: Engine,
    ids: list[str],
) -> dict[str, list[dict[str, object]]]:
    """Load item ids and translated names for each variant id."""
    if not ids:
        return {}

    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT vi.variant_id, vi.item_id, i.name::string AS name "
                + "FROM public.variants_items vi "
                + "JOIN public.items i ON i.id = vi.item_id "
                + f"WHERE vi.variant_id IN ('{ids_join}') "
                + "ORDER BY vi.variant_id, vi.item_id"
            )
        ).fetchall()

    items_by_variant: dict[str, list[dict[str, object]]] = {}
    for variant_id, item_id, name in rows:
        key = str(variant_id)
        items_by_variant.setdefault(key, []).append(
            {
                "id": str(item_id),
                "name": json.loads(str(name)),
            }
        )
    return items_by_variant


def prepend_item_names(
    desc: Mapping[str, object],
    items: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    merged_desc = dict(desc)
    for lang in SUPPORTED_LANGS:
        translated_names: list[str] = []
        seen_names: set[str] = set()
        for item in items:
            item_name = item.get("name")
            if not isinstance(item_name, Mapping):
                continue
            name = item_name.get(lang)
            if lang == DEFAULT_LANG and not name:
                name = item_name.get("xx")
            if not isinstance(name, str):
                continue
            stripped_name = name.strip()
            if stripped_name == "" or stripped_name in seen_names:
                continue
            seen_names.add(stripped_name)
            translated_names.append(stripped_name)
            if len(translated_names) >= MAX_ITEM_DESC_COUNT:
                break

        if not translated_names:
            continue

        prefix = f"Items:\n{'\n'.join(translated_names)}\n"
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


def effective_variant_name(name: Mapping[str, object]) -> str | None:
    english_name = name.get(DEFAULT_LANG)
    if not isinstance(english_name, str) or english_name.strip() == "":
        english_name = name.get("xx")
    if not isinstance(english_name, str):
        return None
    normalized = english_name.strip()
    if normalized == "":
        return None
    return normalized


def should_index_variant(name: Mapping[str, object]) -> bool:
    effective_name = effective_variant_name(name)
    if effective_name is None:
        return False
    if len(effective_name) < 3:
        return False
    return not effective_name.isdigit()


def index_variants(
    crdb: Engine,
    ts: typesense_sdk.Client,
    keys: list[str],
    collection_suffix: str | None = None,
):
    """
    Index the variants in Typesense.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.variants",
        ids=keys,
        cols='id, updated_at, name::string, "desc"::string, code',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.variants")
        print(f"Columns: {df.describe()}")
        df = with_unix_timestamps(df, ["updated_at"])
        docs = df.to_dicts()
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
        docs = [doc for doc in docs if should_index_variant(doc["name"])]
        items_by_id = load_items_by_variant_ids(
            crdb,
            [str(doc["id"]) for doc in docs],
        )
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.variants_tags",
            "variant_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            items = items_by_id.get(str(doc["id"]), [])
            doc["desc"] = prepend_item_names(doc["desc"], items)
            item_ids = [
                item_id
                for item in items
                if isinstance((item_id := item.get("id")), str)
            ]
            if item_ids:
                doc["items"] = item_ids
            doc["code"] = barcode_forms(doc.get("code"))
            if doc["code"] is None:
                del doc["code"]
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        import_documents(
            ts,
            resolve_collection_name("variants", collection_suffix),
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
            "variants",
            COLLECTION_FIELDS,
            collection_suffix=collection_suffix,
        )
    index_variants(crdb, ts, keys, collection_suffix=collection_suffix)
