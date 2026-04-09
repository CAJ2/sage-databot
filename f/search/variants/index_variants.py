# requirements: project

from sqlalchemy import Engine
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
from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)

LANG_FIELDS = ["name", "desc"]
STANDARD_BARCODE_LENGTHS = [7, 8, 12, 13, 14]
COLLECTION_FIELDS = [
    {"name": "id", "type": "string"},
    {"name": "updated_at", "type": "int64", "sort": True},
    {"name": "code", "type": "string[]", "optional": True},
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
        tags_by_id = load_tags_by_entity_ids(
            crdb,
            "public.variants_tags",
            "variant_id",
            [str(doc["id"]) for doc in docs],
        )
        for doc in docs:
            doc["name"] = json.loads(str(doc["name"]))
            doc["desc"] = json.loads(str(doc["desc"] or "{}"))
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
