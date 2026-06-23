# requirements: project

from typing import Any

from f.changes.apply import main as apply_main


def _unique_ids(target_ids: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for target_id in target_ids:
        normalized = target_id.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def build_apply_requests(
    source_entity_name: str,
    source_entity_id: str,
    target_ids: list[str],
) -> list[dict[str, Any]]:
    normalized_ids = _unique_ids(target_ids)
    if not normalized_ids:
        return []

    if source_entity_name == "Item":
        return [
            {
                "entity_name": "Variant",
                "entity_id": target_id,
                "data": {"add_items": [source_entity_id]},
            }
            for target_id in normalized_ids
        ]

    if source_entity_name == "Variant":
        return [
            {
                "entity_name": "Variant",
                "entity_id": source_entity_id,
                "data": {
                    "add_components": [
                        {"id": target_id} for target_id in normalized_ids
                    ]
                },
            }
        ]

    raise ValueError(f"Unsupported source_entity_name: {source_entity_name}")


def main(
    source_entity_name: str,
    source_entity_id: str,
    target_ids: list[str],
    apply: bool = False,
    change_id: str | None = None,
    change_title: str | None = None,
) -> dict[str, Any]:
    requests = build_apply_requests(source_entity_name, source_entity_id, target_ids)
    if not requests:
        return {
            "association_count": 0,
            "target_ids": [],
            "change_id": change_id,
            "applied": apply,
            "results": [],
        }

    current_change_id = change_id
    results: list[dict[str, Any]] = []
    for request in requests:
        result = apply_main(
            entity_name=request["entity_name"],
            entity_id=request["entity_id"],
            data=request["data"],
            apply=apply,
            change_id=current_change_id,
            change_title=change_title,
        )
        results.append(result)
        if result["change_id"]:
            current_change_id = result["change_id"]

    return {
        "association_count": len(_unique_ids(target_ids)),
        "target_ids": _unique_ids(target_ids),
        "change_id": current_change_id,
        "applied": apply,
        "results": results,
    }
