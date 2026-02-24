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
    Analyzes a Process edit using AI. Fetches the linked Material, Place, Org,
    Region, and Variant for context about the relational changes.
    """
    client, _ = api_connect()
    model = llm_agent()

    entity_id = original_id or proposed_id
    changes_data = update_changes or create_changes or {}

    # Fetch current process context
    process_context = None
    if entity_id:
        try:
            result = client.get_process_for_review(id=entity_id)
            process_context = result.process.model_dump(by_alias=False) if result.process else None
        except Exception as e:
            print(f"Could not fetch process context for {entity_id}: {e}")

    # Fetch linked material context if being changed
    material_context = None
    material_input = changes_data.get("material")
    if material_input and "id" in material_input:
        try:
            result = client.get_material_for_review(id=material_input["id"])
            material_context = result.material.model_dump(by_alias=False) if result.material else None
        except Exception as e:
            print(f"Could not fetch material {material_input['id']}: {e}")

    # Fetch linked place context if being changed
    place_context = None
    place_input = changes_data.get("place")
    if place_input and "id" in place_input:
        try:
            result = client.get_place_for_review(id=place_input["id"])
            place_context = result.place.model_dump(by_alias=False) if result.place else None
        except Exception as e:
            print(f"Could not fetch place {place_input['id']}: {e}")

    if create_changes:
        change_description = f"CREATE a new Process with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = f"UPDATE Process (id={entity_id}).\nChanged fields: {json.dumps(update_changes, indent=2)}"
    else:
        change_description = f"Unknown edit type for Process (id={entity_id})."

    context = {
        "current_process": process_context,
        "material_being_linked": material_context,
        "place_being_linked": place_context,
    }

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
    )
