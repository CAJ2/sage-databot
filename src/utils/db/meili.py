from prefect.variables import Variable
from prefect.blocks.system import Secret
import meilisearch


def meili_connect() -> meilisearch.Client:
    # Connect to Meilisearch
    try:
        meili_key = Secret.load("meilisearch-api-key")
    except Exception as e:
        meili_key = None
    meili = meilisearch.Client(
        Variable.get("meilisearch", default="http://localhost:7700"),
        api_key=meili_key.get() if meili_key else None,
    )
    health = meili.is_healthy()
    if not health:
        raise ValueError("Meilisearch is not healthy or not reachable.")
    return meili
