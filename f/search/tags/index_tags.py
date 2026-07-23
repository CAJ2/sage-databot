# requirements: project

from f.search.tags.rank_tags import main as rank_main


def main(
    keys: list[str],
    check: bool = True,
    collection_suffix: str | None = None,
) -> dict[str, int]:
    _ = check, collection_suffix
    return rank_main(keys)
