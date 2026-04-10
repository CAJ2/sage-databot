# requirements: project

from f.search.index_script import ALLOWED_TABLES
from f.utils.db.typesense import flip_alias, ts_connect


def main(table: str, collection_suffix: str) -> dict[str, str]:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table}")
    suffix = collection_suffix.strip()
    if suffix == "":
        raise ValueError("collection_suffix is required to flip aliases")
    ts = ts_connect()
    flip_alias(ts, table, suffix)
    print(f"Flipped alias for {table} to suffix {suffix}")
    return {"table": table, "collection_suffix": suffix}
