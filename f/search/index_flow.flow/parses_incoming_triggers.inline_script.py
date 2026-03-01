from typing import TypedDict


class ChangeSet(TypedDict):
    table: str
    keys: list[str]


def main(payload: list, length: int):
    if len(payload) != length:
        raise ValueError("invalid length")
    changes = dict()
    for p in payload:
        table = p["topic"]
        if table not in changes:
            changes[table] = ChangeSet(table=table, keys=[])
        key = p["key"][0]
        changes[table]["keys"].append(key)
    return list(changes.values())
