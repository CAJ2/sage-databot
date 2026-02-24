# requirements: project

import json

from f.utils.general import llm_agent
from f.utils.api import api_connect
from f.changes.ai_review import EditAnalysis, analyze_edit


def main(
    change_id: str,
    edit_id: str,
    entity_name: str,
    create_changes: dict | None,
    update_changes: dict | None,
    proposed_id: str | None,
    original_id: str | None,
) -> EditAnalysis:
    """
    Analyzes a Variant edit using AI. Fetches the variant's linked Items,
    Components, and Orgs as contextual information.
    For relational edits (e.g. adding/removing Item links), fetches those
    Items to verify the connection makes sense.
    """
    client, _ = api_connect()
    model = llm_agent()

    entity_id = original_id or proposed_id

    # Fetch current variant context
    variant_context = None
    if entity_id:
        try:
            result = client.get_variant_for_review(id=entity_id)
            variant_context = result.variant.model_dump(by_alias=False) if result.variant else None
        except Exception as e:
            print(f"Could not fetch variant context for {entity_id}: {e}")

    # Fetch context for any items being linked
    item_contexts = {}
    changes_data = update_changes or create_changes or {}
    item_ids_to_link = []
    if "addItems" in changes_data:
        item_ids_to_link = [i["id"] for i in changes_data["addItems"] if "id" in i]
    elif "items" in changes_data:
        item_ids_to_link = [i["id"] for i in changes_data["items"] if "id" in i]

    for item_id in item_ids_to_link[:5]:
        try:
            result = client.get_item_for_link(id=item_id)
            if result.item:
                item_contexts[item_id] = result.item.model_dump(by_alias=False)
        except Exception as e:
            print(f"Could not fetch item {item_id}: {e}")

    if create_changes:
        change_description = f"CREATE a new Variant with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = f"UPDATE Variant (id={entity_id}).\nChanged fields: {json.dumps(update_changes, indent=2)}"
        if item_ids_to_link:
            change_description += f"\n\nItems being linked: {item_ids_to_link}"
    else:
        change_description = f"Unknown edit type for Variant (id={entity_id})."

    context = {
        "current_variant": variant_context,
        "items_being_linked": item_contexts,
    }

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
    )
