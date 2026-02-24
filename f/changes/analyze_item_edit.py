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
    Analyzes an Item edit using AI. Fetches the item's categories and variants
    as contextual information for the review.
    """
    client, _ = api_connect()
    model = llm_agent()

    entity_id = original_id or proposed_id

    # Fetch current item context
    item_context = None
    if entity_id:
        try:
            result = client.get_item_for_review(id=entity_id)
            item_context = result.item.model_dump(by_alias=False) if result.item else None
        except Exception as e:
            print(f"Could not fetch item context for {entity_id}: {e}")

    if create_changes:
        change_description = f"CREATE a new Item with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = (
            f"UPDATE Item (id={entity_id}).\n"
            f"Changed fields: {json.dumps(update_changes, indent=2)}"
        )
    else:
        change_description = f"Unknown edit type for Item (id={entity_id})."

    context = {"current_item": item_context}

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
    )
