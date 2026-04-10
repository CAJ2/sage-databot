# requirements: project

import base64
import json
from collections.abc import Mapping, Sequence
from io import BytesIO
from urllib.request import urlopen

import typesense as typesense_sdk
from PIL import Image, ImageOps
from sqlalchemy import Engine, text

from f.utils.db.crdb import (
    create_sql_engine,
    export_table_by_ids,
    load_tags_by_entity_ids,
)
from f.utils.db.typesense import (
    DEFAULT_LANG,
    SUPPORTED_LANGS,
    add_mistral_embeddings,
    check_create_aliased_collection,
    expand_translated_docs,
    import_documents,
    mistral_embedding_field,
    resolve_collection_name,
    translated_field_names,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)

LANG_FIELDS = ["name", "desc"]
EMBED_FIELDS = translated_field_names(LANG_FIELDS)
STANDARD_BARCODE_LENGTHS = [7, 8, 12, 13, 14]
MAX_ITEM_DESC_COUNT = 5
IMAGE_CONTEXT_MAX_CHARS = 150
SOURCES_CDN_PREFIX = "cdn://sources/"
SOURCES_PUBLIC_BASE_URL = "https://sources.sageleaf.app/"


def collection_fields() -> list[dict[str, object]]:
    return [
        {"name": "updated_at", "type": "int64", "sort": True},
        {"name": "code", "type": "string[]", "optional": True},
        {"name": "image", "type": "image", "optional": True, "store": False},
        {
            "name": "image_embedding",
            "type": "float[]",
            "optional": True,
            "embed": {
                "from": ["image"],
                "model_config": {"model_name": "ts/clip-vit-b-p32"},
            },
        },
        {"name": "components", "type": "string[]", "optional": True, "facet": True},
        {"name": "items", "type": "string[]", "optional": True, "facet": True},
        {"name": "tags", "type": "string[]", "optional": True, "facet": True},
        *translated_schema_fields({"name": "string", "desc": "string"}),
        mistral_embedding_field(EMBED_FIELDS),
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


def load_components_by_variant_ids(
    crdb: Engine,
    ids: list[str],
) -> dict[str, list[dict[str, object]]]:
    """Load component ids and translated names for each variant id."""
    if not ids:
        return {}

    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT vc.variant_id, vc.component_id, c.name::string AS name "
                + "FROM public.variants_components vc "
                + "JOIN public.components c ON c.id = vc.component_id "
                + f"WHERE vc.variant_id IN ('{ids_join}') "
                + "ORDER BY vc.variant_id, vc.component_id"
            )
        ).fetchall()

    components_by_variant: dict[str, list[dict[str, object]]] = {}
    for variant_id, component_id, name in rows:
        key = str(variant_id)
        components_by_variant.setdefault(key, []).append(
            {
                "id": str(component_id),
                "name": json.loads(str(name)),
            }
        )
    return components_by_variant


def load_primary_image_data_by_variant_ids(
    crdb: Engine,
    ids: list[str],
) -> dict[str, dict[str, str]]:
    """Load the preferred 400px primary image URL and OCR context for each variant."""
    if not ids:
        return {}

    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT vs.variant_id, COALESCE(s.location, s.content_url) AS image_url, "
                + "s.content->>'context' AS image_context "
                + "FROM public.variants_sources vs "
                + "JOIN public.sources s ON s.id = vs.source_id "
                + f"WHERE vs.variant_id IN ('{ids_join}') "
                + "AND vs.meta->>'order' = '1' "
                + "AND s.metadata->>'size' = '400' "
                + "AND s.type = 'IMAGE' "
                + "AND COALESCE(s.location, s.content_url) IS NOT NULL "
                + "ORDER BY vs.variant_id, vs.source_id"
            )
        ).fetchall()

    image_data_by_variant: dict[str, dict[str, str]] = {}
    for variant_id, image_url, image_context in rows:
        key = str(variant_id)
        if key not in image_data_by_variant:
            image_data_by_variant[key] = {"url": normalize_source_url(str(image_url))}
            if isinstance(image_context, str) and image_context.strip() != "":
                image_data_by_variant[key]["context"] = image_context.strip()
    return image_data_by_variant


def normalize_source_url(source_url: str) -> str:
    if source_url.startswith(SOURCES_CDN_PREFIX):
        return source_url.replace(SOURCES_CDN_PREFIX, SOURCES_PUBLIC_BASE_URL, 1)
    return source_url


def prepend_related_names(
    desc: Mapping[str, object],
    items: Sequence[Mapping[str, object]],
    label: str,
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

        prefix = f"{label}:\n{'\n'.join(translated_names)}\n"
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


def prepend_item_names(
    desc: Mapping[str, object],
    items: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return prepend_related_names(desc, items, "Items")


def prepend_component_names(
    desc: Mapping[str, object],
    components: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return prepend_related_names(desc, components, "Components")


def append_image_context(
    desc: Mapping[str, object],
    image_context: str,
) -> dict[str, object]:
    stripped_context = image_context.strip()[:IMAGE_CONTEXT_MAX_CHARS]
    if stripped_context == "":
        return dict(desc)

    merged_desc = dict(desc)
    current = merged_desc.get(DEFAULT_LANG, "")
    if not isinstance(current, str) or current.strip() == "":
        current = merged_desc.get("xx", "")
    if not isinstance(current, str):
        current = ""

    current_text = current.strip()
    context_block = f"Image:\n{stripped_context}"
    merged_desc[DEFAULT_LANG] = (
        f"{current_text}\n{context_block}" if current_text else context_block
    )
    return merged_desc


def clip_image_base64(image_bytes: bytes) -> str:
    with Image.open(BytesIO(image_bytes)) as image:
        clipped = ImageOps.fit(
            image.convert("RGB"),
            (224, 224),
            method=Image.Resampling.LANCZOS,
        )
        out = BytesIO()
        clipped.save(out, format="JPEG", quality=90)
    return base64.b64encode(out.getvalue()).decode("ascii")


def fetch_clip_image_base64(image_url: str) -> str:
    with urlopen(image_url, timeout=20) as response:
        return clip_image_base64(response.read())


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
        components_by_id = load_components_by_variant_ids(
            crdb,
            [str(doc["id"]) for doc in docs],
        )
        image_data_by_id = load_primary_image_data_by_variant_ids(
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
            components = components_by_id.get(str(doc["id"]), [])
            doc["desc"] = prepend_item_names(doc["desc"], items)
            doc["desc"] = prepend_component_names(doc["desc"], components)
            item_ids = [
                item_id
                for item in items
                if isinstance((item_id := item.get("id")), str)
            ]
            if item_ids:
                doc["items"] = item_ids
            component_ids = [
                component_id
                for component in components
                if isinstance((component_id := component.get("id")), str)
            ]
            if component_ids:
                doc["components"] = component_ids
            doc["code"] = barcode_forms(doc.get("code"))
            if doc["code"] is None:
                del doc["code"]
            image_data = image_data_by_id.get(str(doc["id"]))
            if image_data:
                image_context = image_data.get("context")
                if image_context:
                    doc["desc"] = append_image_context(doc["desc"], image_context)
                image_url = image_data.get("url")
                if image_url:
                    doc["image"] = fetch_clip_image_base64(image_url)
            tags = tags_by_id.get(str(doc["id"]))
            if tags:
                doc["tags"] = tags
        expanded_docs = expand_translated_docs(docs, LANG_FIELDS)
        import_documents(
            ts,
            resolve_collection_name("variants", collection_suffix),
            add_mistral_embeddings(expanded_docs, EMBED_FIELDS),
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
            collection_fields(),
            collection_suffix=collection_suffix,
        )
    index_variants(crdb, ts, keys, collection_suffix=collection_suffix)
