import time
from typing import Any, cast
import wmill
import meilisearch
from stopwordsiso import stopwords
import copy
import iso639

SUPPORTED_LANGS = ["en", "fr", "sv"]

index_settings: dict[str, Any] = {
    "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness",
    ],
    "sortableAttributes": ["updated_at"],
    "stopWords": list(stopwords(SUPPORTED_LANGS)),
}


def meili_connect() -> meilisearch.Client:
    # Connect to Meilisearch
    meili_res = wmill.get_resource("f/api_config/api_meilisearch")
    if meili_res is None:
        raise ValueError("Unable to find meilisearch resource")
    meili = meilisearch.Client(
        str(meili_res["api_url"]),
        api_key=meili_res.get("api_key", None),
    )
    health = meili.is_healthy()
    if not health:
        raise ValueError("Meilisearch is not healthy or not reachable.")
    return meili


class MeiliClient:
    _client: meilisearch.Client

    def __init__(self, client: meilisearch.Client):
        self._client = client

    @property
    def client(self) -> meilisearch.Client:
        return self._client

    def search(
        self,
        index: str,
        query: str,
        params: dict[str, Any] | None = None,
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> dict[str, Any]:
        """Search an index with automatic retry on failure."""
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                return self._client.index(index).search(query, params or {})
            except Exception as e:
                last_exc = e
                if attempt < retries - 1:
                    print(
                        f"Meilisearch search failed (attempt {attempt + 1}/{retries}): {e}"
                    )
                    time.sleep(retry_delay)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Meilisearch search failed with no retries")

    def ranking_search(
        self,
        index: str,
        query: str,
        threshold: float = 0.5,
        limit: int = 1,
        filter: str | None = None,
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> list[dict[str, Any]]:
        """Search with a ranking score threshold. Returns the hits list."""
        params: dict[str, Any] = {"rankingScoreThreshold": threshold, "limit": limit}
        if filter is not None:
            params["filter"] = filter
        result = self.search(
            index,
            query,
            params,
            retries=retries,
            retry_delay=retry_delay,
        )
        return cast(list[dict[str, Any]], result.get("hits", []))

    def multi_search(
        self,
        queries: list[dict[str, Any]],
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> list[dict[str, Any]]:
        """Search across multiple indexes. Returns list of result dicts per query."""
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                result = self._client.multi_search(queries)
                return result.get("results", [])
            except Exception as e:
                last_exc = e
                if attempt < retries - 1:
                    print(
                        f"Meilisearch multi_search failed (attempt {attempt + 1}/{retries}): {e}"
                    )
                    time.sleep(retry_delay)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("Meilisearch multi_search failed with no retries")


def meili_client() -> MeiliClient:
    """Return a MeiliClient wrapping the standard meili connection."""
    return MeiliClient(meili_connect())


def check_lang(lang: str) -> str | None:
    """
    Check if the language is valid.
    """
    try:
        if lang == "xx":
            return lang
        language = iso639.Language.match(lang.split("-")[0])
        if language.part1:
            lang = language.part1
        else:
            lang = language.part3
    except Exception:
        return None
    return lang


def check_create_index(
    meili: meilisearch.Client,
    index_name: str,
    settings: dict[str, Any] | None = None,
) -> None:
    settings = settings or {}
    task = meili.create_index(index_name, {"primaryKey": "id"})
    _ = meili.wait_for_task(task.task_uid, timeout_in_ms=120000, interval_in_ms=500)
    settings_copy = copy.deepcopy(index_settings)
    settings_copy.update(settings)
    task = meili.index(index_name).update_settings(settings_copy)
    _ = meili.wait_for_task(task.task_uid, timeout_in_ms=120000, interval_in_ms=5000)


def _normalize_lang_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Strip qualifier suffixes from language keys (e.g. 'sv;a' -> 'sv')."""
    return {k.split(";")[0]: v for k, v in d.items()}


def filter_docs_for_lang(
    docs: list[dict[str, Any]], lang_fields: list[str], lang: str
) -> list[dict[str, Any]]:
    """Return docs that should be stored in the given language index.

    All docs go into the 'en' index. For other languages, only docs that have
    that language key in at least one translatable field are included.
    """
    if lang == "en":
        return docs

    def _has_lang(doc: dict[str, Any]) -> bool:
        for field in lang_fields:
            val = doc.get(field)
            if isinstance(val, dict):
                if lang in _normalize_lang_keys(val):
                    return True
            elif isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and lang in _normalize_lang_keys(item):
                        return True
        return False

    return [doc for doc in docs if _has_lang(doc)]


def split_docs_by_lang(
    docs: list[dict[str, Any]], lang_fields: list[str], lang: str
) -> list[dict[str, Any]]:
    result = []
    for doc in docs:
        lang_doc: dict[str, Any] = {}
        for k, v in doc.items():
            if k in lang_fields and isinstance(v, dict):
                v = _normalize_lang_keys(v)
                lang_doc[k] = v.get(lang) or v.get("xx", "")
            elif k in lang_fields and isinstance(v, list):
                lang_doc[k] = [
                    (lambda n: n.get(lang) or n.get("xx", ""))(
                        _normalize_lang_keys(item)
                    )
                    if isinstance(item, dict)
                    else item
                    for item in v
                ]
            else:
                lang_doc[k] = v
        result.append(lang_doc)
    return result


def check_create_lang_indexes(
    meili: meilisearch.Client,
    base_name: str,
    settings: dict[str, Any] | None = None,
    lang_fields: list[str] | None = None,
) -> None:
    settings = settings or {}
    lang_fields = lang_fields or []
    for lang in SUPPORTED_LANGS:
        lang_settings = copy.deepcopy(settings)
        lang_settings["stopWords"] = list(stopwords([lang]))
        if lang_fields:
            lang_settings["localizedAttributes"] = [
                {"locales": [lang], "attributePatterns": lang_fields}
            ]
        check_create_index(meili, f"{base_name}_{lang}", lang_settings)
