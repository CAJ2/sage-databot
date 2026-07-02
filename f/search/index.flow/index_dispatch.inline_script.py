# requirements: project

from collections.abc import Callable
from typing import TypedDict

from tenacity import retry, stop_after_attempt, wait_fixed

from f.search.categories.index_categories import main as index_categories
from f.search.regions.index_regions import main as index_regions
from f.search.orgs.index_orgs import main as index_orgs
from f.search.variants.index_variants import main as index_variants
from f.search.components.index_components import main as index_components
from f.search.materials.index_materials import main as index_materials
from f.search.places.index_places import main as index_places
from f.search.items.index_items import main as index_items
from f.search.processes.index_processes import main as index_processes
from f.search.programs.index_programs import main as index_programs
from f.search.tags.index_tags import main as index_tags


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
    "programs": index_programs,
    "items": index_items,
    "processes": index_processes,
    "tags": index_tags,
}


def main(changes: list[ChangeSet], collection_suffix: str = ""):
    results = []
    for change in changes:
        table = change["table"]
        keys = change["keys"]
        handler = TABLE_HANDLERS.get(table)
        if handler is None:
            print(f"No handler for table: {table}, skipping")
            continue

        fn: Callable[[list[str], bool, str | None], object] = handler

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_fixed(5),
            reraise=True,
        )
        def run_with_retry(
            fn: Callable[[list[str], bool, str | None], object] = fn,
            keys: list[str] = keys,
        ) -> None:
            fn(keys, True, collection_suffix or None)

        print(f"Indexing {len(keys)} keys for table: {table}")
        run_with_retry()
        results.append({"table": table, "keys_count": len(keys)})

    return results
