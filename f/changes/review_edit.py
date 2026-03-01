# requirements: project

import json
from typing import Any

from f.changes.ai_review import EditAnalysis, analyze_edit
from f.context.context_types import EntityContext
from f.utils.general import llm_agent


def main(
    entity_context: dict[str, Any],
    edit_id: str,
    create_changes: dict[str, Any] | None,
    update_changes: dict[str, Any] | None,
) -> EditAnalysis:
    """
    Generic AI review of a single edit using pre-fetched EntityContext.
    Accepts entity_context as a plain dict (Windmill serializes Pydantic models to dicts
    across flow steps) and re-validates it into EntityContext.
    """
    ctx = EntityContext.model_validate(entity_context)
    model = llm_agent()

    entity_id = ctx.entity_id
    entity_name = ctx.entity_name

    if create_changes:
        change_description = f"CREATE a new {entity_name} with fields:\n{json.dumps(create_changes, indent=2)}"
    elif update_changes:
        change_description = (
            f"UPDATE {entity_name} (id={entity_id}).\n"
            f"Changed fields: {json.dumps(update_changes, indent=2)}"
        )
    else:
        change_description = f"Unknown edit type for {entity_name} (id={entity_id})."

    context = {
        "entity": ctx.entity_data,
        "related": ctx.related_data,
    }

    return analyze_edit(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        change_description=change_description,
        context=context,
        model=model,
        prompt_hints=ctx.prompt_hints,
    )
