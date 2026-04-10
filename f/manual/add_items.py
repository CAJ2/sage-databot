# requirements: project

from f.graphql.api_client.input_types import CreateItemInput
from f.utils.api import api_connect
from f.utils.db.typesense import ts_client

SEARCH_THRESHOLD = 0.85


def main(names: list[str]) -> list[dict[str, str | None]]:
    """
    Creates one Item per entry in `names` with the name set in English ("en"),
    skipping any name that already has a close match in the Typesense items index.

    Args:
        names: JSON array of strings, each becoming the English name of a new Item.

    Returns:
        List of dicts per input name with ``id``, ``name``, ``change_id``, and
        ``skipped`` (set to the matched item id when skipped, otherwise None).
    """
    client, _ = api_connect()
    ts = ts_client()

    results = []
    for name in names:
        hits = ts.ranking_search(
            "items",
            name,
            threshold=SEARCH_THRESHOLD,
            limit=1,
            query_by="name_en",
        )
        hit = hits[0] if hits else None
        if hit:
            print(
                f"Skipping {name!r}: close match already exists ({hit['id']!r} {hit.get('name_en')!r})"
            )
            results.append(
                {"id": None, "name": name, "change_id": None, "skipped": hit["id"]}
            )
            continue

        input_obj = CreateItemInput(name=name, lang="en", apply=True)
        response = client.add_item(input=input_obj)
        create_item = response.create_item
        item = create_item.item if create_item else None
        change = create_item.change if create_item else None
        results.append(
            {
                "id": item.id if item else None,
                "name": item.name if item else None,
                "change_id": change.id if change else None,
                "skipped": None,
            }
        )
        if item:
            print(f"Created item {item.id!r} with name {name!r}")

    return results
