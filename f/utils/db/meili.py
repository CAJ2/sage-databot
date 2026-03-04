import time
from typing import Any
import wmill
import meilisearch
from meilisearch.errors import MeilisearchApiError
from stopwordsiso import stopwords
import copy

locales = ["en", "sv"]

index_settings = {
    "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness",
    ],
    "sortableAttributes": ["updated_at"],
    "stopWords": list(stopwords(locales)),
    "localizedAttributes": list(
        {"locales": [o], "attributePatterns": ["*." + o]} for o in locales
    ),
}


def meili_connect() -> meilisearch.Client:
    # Connect to Meilisearch
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
        raise last_exc  # type: ignore[misc]

    def ranking_search(
        self,
        index: str,
        query: str,
        threshold: float = 0.5,
        limit: int = 1,
        retries: int = 3,
        retry_delay: float = 5.0,
    ) -> list[dict[str, Any]]:
        """Search with a ranking score threshold. Returns the hits list."""
        result = self.search(
            index,
            query,
            {"rankingScoreThreshold": threshold, "limit": limit},
            retries=retries,
            retry_delay=retry_delay,
        )
        return result.get("hits", [])

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
        raise last_exc  # type: ignore[misc]


def meili_client() -> MeiliClient:
    """Return a MeiliClient wrapping the standard meili connection."""
    return MeiliClient(meili_connect())


def check_create_index(
    meili: meilisearch.Client, index_name: str, settings: dict[str, Any] = {}
):
    try:
        meili.get_index(index_name)
    except MeilisearchApiError:
        return
    op = meili.create_index(index_name, {"primaryKey": "id"})
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=500)
    settings_copy = copy.deepcopy(index_settings)
    settings_copy.update(settings)
    op = meili.index(index_name).update_settings(settings_copy)
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=5000)
