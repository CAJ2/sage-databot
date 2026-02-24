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
    Analyzes a Category edit using AI. Fetches the category's parent/child
    categories and associated items as context.
    """
    client, _ = api_connect()
    model = llm_agent()

    entity_id = original_id or proposed_id

    category_context = None
    if entity_id:
        try:
            result = client.get_category_for_review(id=entity_id)
            category_context = result.category.model_dump(by_alias=False) if result.category else None
        except Exception as e:
            print(f"Could not fetch category context for {entity_id}: {e}")

    if create_changes:
        change_description = f"CREATE a new Category with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = f"UPDATE Category (id={entity_id}).\nChanged fields: {json.dumps(update_changes, indent=2)}"
    else:
        change_description = f"Unknown edit type for Category (id={entity_id})."

    context = {"current_category": category_context}

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
    )
