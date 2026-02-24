# requirements: project

import json

from f.utils.general import llm_agent
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
    Generic fallback analyzer for model types without a dedicated script
    (e.g. Material, Region). Uses the raw change data without fetching
    additional context.
    """
    model = llm_agent()

    entity_id = original_id or proposed_id

    if create_changes:
        change_description = f"CREATE a new {entity_name} with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = f"UPDATE {entity_name} (id={entity_id}).\nChanged fields: {json.dumps(update_changes, indent=2)}"
    else:
        change_description = f"Unknown edit type for {entity_name} (id={entity_id})."

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context={},
        model=model,
    )
