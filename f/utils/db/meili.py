import wmill
import meilisearch
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
        meili_res.get("api_url"),
        api_key=meili_res.get("api_key", None),
    )
    health = meili.is_healthy()
    if not health:
        raise ValueError("Meilisearch is not healthy or not reachable.")
    return meili


def check_create_index(meili: meilisearch.Client, index_name: str, settings: dict = {}):
    try:
        meili.get_index(index_name)
    except meilisearch.errors.MeiliSearchApiError:
        return
    op = meili.create_index(index_name, {"primaryKey": "id"})
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=500)
    settings_copy = copy.deepcopy(index_settings)
    settings_copy.update(settings)
    op = meili.index(index_name).update_settings(settings_copy)
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=5000)
