# requirements: project

from typing import Any

from f.assoc.name_match import name_match
from f.graphql.api_client import Client
from f.graphql.api_client.enums import SearchType
from f.graphql.api_client.input_types import (
    CreateChangeInput,
    UpdateVariantInput,
)
from f.utils.api import api_connect


def _existing_item_ids_on_variant(client: Client, variant_id: str) -> set[str]:
    res = client.get_variant_for_review(variant_id)
    if not res.variant:
        return set()
    return {n.id for n in res.variant.items.nodes}


def item_variants_assoc(
    client: Client,
    item_id: str,
    change_id: str | None = None,
    change_title: str | None = None,
    apply: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    item = client.get_item_for_link(item_id)
    if not item.item or not item.item.name:
        return {
            "item_id": item_id,
            "candidate_count": 0,
            "associated": [],
            "skipped": [],
            "change_id": change_id,
            "applied": apply,
        }

    name = item.item.name
    search = client.search(query=f'"{name}"', types=[SearchType.VARIANT], limit=limit)

    candidates: list[str] = []
    for node in search.search.nodes:
        if node.typename__ != "Variant":
            continue
        if node.id == item_id:
            continue
        if node.name and name_match(node.name, name):
            candidates.append(node.id)

    associated: list[str] = []
    skipped: list[str] = []
    current_change_id = change_id
    for vid in candidates:
        existing = _existing_item_ids_on_variant(client, vid)
        if item_id in existing:
            skipped.append(vid)
            continue
        if apply:
            chg: dict[str, Any] = {"apply": True}
        elif current_change_id:
            chg = {"changeID": current_change_id}
        else:
            chg = {
                "change": CreateChangeInput(
                    title=change_title or f"Auto-associate Variants to Item '{name}'"
                )
            }
        payload = UpdateVariantInput.model_validate(
            {"id": vid, "addItems": [{"id": item_id}], **chg}
        )
        resp = client.update_variant(input=payload)
        associated.append(vid)
        if resp.update_variant.change:
            current_change_id = resp.update_variant.change.id

    return {
        "item_id": item_id,
        "candidate_count": len(candidates),
        "associated": associated,
        "skipped": skipped,
        "change_id": current_change_id,
        "applied": apply,
    }


def main(
    item_id: str,
    change_id: str | None = None,
    change_title: str | None = None,
    apply: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    client, _ = api_connect()
    return item_variants_assoc(
        client,
        item_id,
        change_id=change_id,
        change_title=change_title,
        apply=apply,
        limit=limit,
    )
