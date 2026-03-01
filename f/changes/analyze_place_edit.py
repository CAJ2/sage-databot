# requirements: project

import json
from typing import Any

from f.utils.general import llm_agent
from f.utils.api import api_connect
from f.changes.ai_review import EditAnalysis, analyze_edit


def main(
    change_id: str,
    edit_id: str,
    entity_name: str,
    create_changes: dict[str, Any] | None,
    update_changes: dict[str, Any] | None,
    proposed_id: str | None,
    original_id: str | None,
) -> EditAnalysis:
    """
    Analyzes a Place edit using AI. Fetches current place state including
    address, location, and linked org as context.
    """
    client, _ = api_connect()
    model = llm_agent()

    entity_id = original_id or proposed_id

    place_context = None
    if entity_id:
        try:
            result = client.get_place_for_review(id=entity_id)
            place_context = (
                result.place.model_dump(by_alias=False) if result.place else None
            )
        except Exception as e:
            print(f"Could not fetch place context for {entity_id}: {e}")

    if create_changes:
        change_description = (
            f"CREATE a new Place with fields:\n{json.dumps(create_changes, indent=2)}"
        )
    elif update_changes:
        change_description = f"UPDATE Place (id={entity_id}).\nChanged fields: {json.dumps(update_changes, indent=2)}"
    else:
        change_description = f"Unknown edit type for Place (id={entity_id})."

    context = {"current_place": place_context}

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
    )
