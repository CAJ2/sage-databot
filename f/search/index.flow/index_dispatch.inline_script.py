# requirements: project

from collections.abc import Callable
from typing import TypedDict

from tenacity import retry, stop_after_attempt, wait_exponential

from f.search.categories.index_categories import main as index_categories
from f.search.regions.index_regions import main as index_regions
from f.search.orgs.index_orgs import main as index_orgs
from f.search.variants.index_variants import main as index_variants
from f.search.components.index_components import main as index_components
from f.search.materials.index_materials import main as index_materials
from f.search.places.index_places import main as index_places
from f.search.items.index_items import main as index_items


class ChangeSet(TypedDict):
    table: str
    keys: list[str]


TABLE_HANDLERS = {
    "categories": index_categories,
    "regions": index_regions,
    "orgs": index_orgs,
    "variants": index_variants,
    "components": index_components,
    "materials": index_materials,
    "places": index_places,
    "items": index_items,
}


def main(changes: list[ChangeSet]):
    results = []
    for change in changes:
        table = change["table"]
        keys = change["keys"]
        handler = TABLE_HANDLERS.get(table)
        if handler is None:
            print(f"No handler for table: {table}, skipping")
            continue

        fn: Callable[[list[str]], None] = handler

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=5, max=5),
            reraise=True,
        )
        def run_with_retry():
            fn(keys)

        print(f"Indexing {len(keys)} keys for table: {table}")
        run_with_retry()
        results.append({"table": table, "keys_count": len(keys)})

    return results
