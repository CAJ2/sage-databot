# requirements: project

from __future__ import annotations

import json
import time
from typing import Any, cast

import polars as pl
import typesense as typesense_sdk
import wmill
from typesense import exceptions as typesense_exceptions

SUPPORTED_LANGS = ["en", "sv", "fr"]
DEFAULT_LANG = "en"
FALLBACK_LANG = "xx"
BOOTSTRAP_SUFFIX = "v1"


def check_lang(lang: str | None) -> str | None:
    if lang is None:
        return None
    normalized = lang.strip().lower()
    if normalized == "":
        return None
    if normalized == FALLBACK_LANG:
        return normalized
    if len(normalized) == 2 and normalized.isalpha():
        return normalized
    return None


def _coerce_node(node: object) -> dict[str, str]:
    if not isinstance(node, dict):
        raise ValueError("Typesense nodes must be JSON objects")

    host = node.get("host")
    if not host:
        raise ValueError("Typesense node object is missing host")

    return {
        "host": str(host),
        "port": str(node.get("port", 8108)),
        "protocol": str(node.get("protocol", "http")),
    }


def parse_typesense_nodes(raw_nodes: str) -> list[dict[str, str]]:
    nodes_value = raw_nodes.strip()
    if nodes_value == "":
        raise ValueError("Typesense nodes variable is empty")

    parsed_nodes = json.loads(nodes_value)

    if isinstance(parsed_nodes, list):
        nodes = [_coerce_node(node) for node in parsed_nodes]
    else:
        nodes = [_coerce_node(parsed_nodes)]

    if not nodes:
        raise ValueError("Typesense nodes variable did not contain any nodes")

    return nodes


def _typesense_nodes() -> list[dict[str, str]]:
    return parse_typesense_nodes(wmill.get_variable("f/api_config/api_typesense_nodes"))


def ts_connect() -> typesense_sdk.Client:
    api_key = wmill.get_variable("f/api_config/api_typesense_key")
    if api_key.strip() == "":
        raise ValueError("Unable to find Typesense API key variable")

    client = typesense_sdk.Client(
        {
            "nodes": cast(Any, _typesense_nodes()),
            "api_key": api_key,
            "connection_timeout_seconds": 2,
        },
    )

    if not client.operations.is_healthy():
        raise ValueError("Typesense is not healthy or not reachable.")

    return client


def ts_client() -> "TypesenseClient":
    return TypesenseClient(ts_connect())


def build_versioned_collection_name(
    alias: str,
    suffix: str | None = None,
) -> str:
    return f"{alias}__{suffix or BOOTSTRAP_SUFFIX}"


def resolve_collection_name(
    base_name: str,
    collection_suffix: str | None = None,
) -> str:
    if collection_suffix:
        return build_versioned_collection_name(base_name, collection_suffix)
    return base_name


def upsert_alias(
    client: typesense_sdk.Client,
    alias: str,
    collection_name: str,
) -> None:
    client.aliases.upsert(alias, {"collection_name": collection_name})


def flip_alias(
    client: typesense_sdk.Client,
    base_name: str,
    collection_suffix: str,
) -> None:
    upsert_alias(
        client,
        base_name,
        build_versioned_collection_name(base_name, collection_suffix),
    )


def _ensure_bootstrap_alias(
    client: typesense_sdk.Client,
    alias: str,
    fields: list[dict[str, Any]],
) -> None:
    try:
        _ = client.aliases[alias].retrieve()
        return
    except typesense_exceptions.ObjectNotFound:
        target = build_versioned_collection_name(alias)
        check_create_collection(client, target, fields)
        upsert_alias(client, alias, target)


def check_create_collection(
    client: typesense_sdk.Client,
    collection_name: str,
    fields: list[dict[str, Any]],
) -> None:
    try:
        client.collections.create(
            cast(
                Any,
                {
                    "name": collection_name,
                    "fields": fields,
                },
            )
        )
    except typesense_exceptions.ObjectAlreadyExists:
        return


def check_create_aliased_collection(
    client: typesense_sdk.Client,
    base_name: str,
    fields: list[dict[str, Any]],
    collection_suffix: str | None = None,
) -> None:
    collection_name = resolve_collection_name(base_name, collection_suffix)
    if collection_suffix:
        check_create_collection(client, collection_name, fields)
        return
    _ensure_bootstrap_alias(client, base_name, fields)


def translated_schema_fields(
    field_types: dict[str, str],
) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for base_name, field_type in field_types.items():
        for lang in SUPPORTED_LANGS:
            fields.append(
                {
                    "name": f"{base_name}_{lang}",
                    "type": field_type,
                    "locale": lang,
                    "stem": True,
                    "optional": True,
                }
            )
    return fields


def with_unix_timestamps(
    df: pl.DataFrame,
    columns: list[str],
) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column in columns:
        if column not in df.columns:
            continue
        expressions.append(
            pl.col(column).dt.epoch(time_unit="s").cast(pl.Int64).alias(column)
        )
    if not expressions:
        return df
    return df.with_columns(expressions)


def _translated_value(value: object, lang: str) -> object | None:
    if not isinstance(value, dict):
        return None
    translated = value.get(lang)
    if translated not in (None, ""):
        return translated
    if lang == DEFAULT_LANG:
        fallback = value.get(FALLBACK_LANG)
        if fallback not in (None, ""):
            return fallback
    return None


def expand_translated_docs(
    docs: list[dict[str, Any]],
    lang_fields: list[str],
) -> list[dict[str, Any]]:
    expanded_docs: list[dict[str, Any]] = []
    for doc in docs:
        expanded_doc = dict(doc)
        for field in lang_fields:
            value = expanded_doc.pop(field, None)
            if value is None:
                continue
            if isinstance(value, dict):
                for lang in SUPPORTED_LANGS:
                    translated = _translated_value(value, lang)
                    if translated not in (None, ""):
                        expanded_doc[f"{field}_{lang}"] = translated
                continue
            if isinstance(value, list):
                for lang in SUPPORTED_LANGS:
                    translated_items: list[object] = []
                    for item in value:
                        translated = _translated_value(item, lang)
                        if translated not in (None, ""):
                            translated_items.append(translated)
                    if translated_items:
                        expanded_doc[f"{field}_{lang}"] = translated_items
        expanded_docs.append(expanded_doc)
    return expanded_docs


def filter_docs_for_lang(
    docs: list[dict[str, Any]],
    lang: str,
    lang_fields: list[str],
) -> list[dict[str, Any]]:
    checked_lang = check_lang(lang)
    if checked_lang is None:
        raise ValueError(f"Invalid language: {lang}")

    filtered_docs: list[dict[str, Any]] = []
    for doc in docs:
        filtered_doc = dict(doc)
        for field in lang_fields:
            value = filtered_doc.get(field)
            if isinstance(value, dict):
                translated = _translated_value(value, checked_lang)
                if translated not in (None, ""):
                    filtered_doc[field] = translated
                else:
                    filtered_doc.pop(field, None)
            elif isinstance(value, list):
                translated_items: list[object] = []
                for item in value:
                    translated = _translated_value(item, checked_lang)
                    if translated not in (None, ""):
                        translated_items.append(translated)
                if translated_items:
                    filtered_doc[field] = translated_items
                else:
                    filtered_doc.pop(field, None)
        filtered_docs.append(filtered_doc)
    return filtered_docs


def split_docs_by_lang(
    docs: list[dict[str, Any]],
    lang_fields: list[str],
) -> dict[str, list[dict[str, Any]]]:
    return {
        lang: filter_docs_for_lang(docs, lang, lang_fields)
        for lang in [
            DEFAULT_LANG,
            *[lang for lang in SUPPORTED_LANGS if lang != DEFAULT_LANG],
        ]
    }


def import_documents(
    client: typesense_sdk.Client,
    collection_name: str,
    docs: list[dict[str, Any]],
) -> None:
    if not docs:
        return

    result = cast(
        list[dict[str, Any]],
        client.collections[collection_name].documents.import_(
            cast(Any, docs),
            cast(Any, {"action": "upsert"}),
        ),
    )
    failures = [row for row in result if not row.get("success")]
    if failures:
        raise ValueError(
            f"Failed to import documents into {collection_name}: {failures}"
        )


def _flatten_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for hit in hits:
        document = dict(cast(dict[str, Any], hit.get("document", hit)))
        if "text_match" in hit:
            document["text_match"] = hit["text_match"]
        flattened.append(document)
    return flattened


def _convert_filter(filter_by: str) -> str:
    if " = " in filter_by:
        field, value = filter_by.split(" = ", 1)
        return f"{field}:={value}"
    return filter_by


class TypesenseClient:
    _client: typesense_sdk.Client

    def __init__(self, client: typesense_sdk.Client):
        self._client = client

    @property
    def client(self) -> typesense_sdk.Client:
        return self._client

    def search(
        self,
        index: str,
        query: str,
        params: dict[str, Any] | None = None,
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> dict[str, Any]:
        last_exc: Exception | None = None
        search_params = {"q": query}
        search_params.update(params or {})
        if "query_by" not in search_params:
            raise ValueError("Typesense search requires query_by")

        if "filter" in search_params:
            search_params["filter_by"] = _convert_filter(
                str(search_params.pop("filter"))
            )

        if "limit" in search_params and "per_page" not in search_params:
            search_params["per_page"] = search_params.pop("limit")

        for attempt in range(retries):
            try:
                result = cast(
                    dict[str, Any],
                    cast(
                        object,
                        self._client.collections[index].documents.search(
                            cast(Any, search_params)
                        ),
                    ),
                )
                hits = cast(list[dict[str, Any]], result.get("hits", []))
                result["hits"] = _flatten_hits(hits)
                return result
            except Exception as exc:
                last_exc = exc
                if attempt < retries - 1:
                    print(
                        f"Typesense search failed (attempt {attempt + 1}/{retries}): {exc}"
                    )
                    time.sleep(retry_delay)

        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Typesense search failed without an exception")

    def ranking_search(
        self,
        index: str,
        query: str,
        threshold: float = 0.0,
        limit: int = 20,
        filter: str | None = None,
        query_by: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        search_params: dict[str, Any] = dict(params or {})
        search_params["per_page"] = limit
        if filter:
            search_params["filter"] = filter
        if query_by:
            search_params["query_by"] = query_by

        result = self.search(index, query, search_params)
        hits = cast(list[dict[str, Any]], result.get("hits", []))
        if not hits:
            return []

        top_score = float(hits[0].get("text_match", 0) or 0)
        if top_score <= 0 or threshold <= 0:
            return hits

        filtered_hits: list[dict[str, Any]] = []
        for hit in hits:
            text_match = float(hit.get("text_match", 0) or 0)
            if text_match / top_score >= threshold:
                filtered_hits.append(hit)
        return filtered_hits
