# requirements: project

from tenacity import retry, stop_after_attempt, wait_fixed

from f.search.categories.rank_categories import main as rank_categories
from f.search.components.rank_components import main as rank_components
from f.search.items.rank_items import main as rank_items
from f.search.orgs.rank_orgs import main as rank_orgs
from f.search.places.rank_places import main as rank_places
from f.search.variants.rank_variants import main as rank_variants
from f.search.processes.rank_processes import main as rank_processes
from f.search.programs.rank_programs import main as rank_programs
from f.search.tags.rank_tags import main as rank_tags

RANK_HANDLERS = {
    "categories": rank_categories,
    "components": rank_components,
    "items": rank_items,
    "orgs": rank_orgs,
    "places": rank_places,
    "programs": rank_programs,
    "variants": rank_variants,
    "processes": rank_processes,
    "tags": rank_tags,
}


def main(table: str, ids: list[str]) -> dict[str, object]:
    handler = RANK_HANDLERS.get(table)
    if handler is None:
        raise ValueError(f"No rank handler for table: {table}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True)
    def run() -> None:
        handler(ids)

    print(f"Ranking {len(ids)} keys for table: {table}")
    run()
    return {"table": table, "keys_count": len(ids)}
