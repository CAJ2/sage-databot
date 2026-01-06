import wmill
import meilisearch


def meili_connect() -> meilisearch.Client:
    # Connect to Meilisearch
    try:
        meili_api = wmill.get_resource("api_meilisearch")
    except Exception as e:
        meili_api = None
    meili = meilisearch.Client(
        meili_api["api_url"] if meili_api else "http://localhost:7700",
        api_key=meili_api["api_key"] if meili_api else None,
    )
    health = meili.is_healthy()
    if not health:
        raise ValueError("Meilisearch is not healthy or not reachable.")
    return meili
