# requirements: project

import json
from typing import Any, cast

from pydantic_ai import Agent

from f.agents.toolsets import search_fixed_type_toolset
from f.changes.ai_shared import (
    FieldSuggestion,
    SuggestResult,
    build_suggestion_model,
)
from f.context.context_types import EntityContext
from f.graphql.api_client.enums import SearchType
from f.utils.general import llm_agent

_SYSTEM_PROMPT = (
    "You are a data management expert for a product and sustainability database. "
    "Given an entity's current data and a reference field to manage, use the search tool "
    "to find relevant entities, then decide what changes to make to the reference field. "
    "For one-to-one ref fields, set the new value directly. "
    "For one-to-many ref fields, suggest which entities to add/change or remove. "
    "The 'data' field in your output should be a schema-compliant mutation payload "
    "using snake_case keys matching the Python input type field names "
    "(e.g. {'add_items':['<id>'],'remove_items':['<id>']} for a many field, "
    "or {'region':{'id':'<id>'}} for a single ref). "
    "Provide concise reasoning for each suggested operation."
)


def main(
    entity_context: dict[str, Any],
    ref_field: str,
    ref_entity_type: str,
    prompt: str | None = None,
    create_prompt: str | None = None,
) -> dict[str, Any]:
    """
    AI agent for managing a single reference field on an entity.
    Searches for related entities, then suggests add/remove/change operations.
    """
    ctx = EntityContext.model_validate(entity_context)

    current_ref = ctx.entity_data.get(ref_field)
    schema = ctx.entity_schema or {}
    schema_excerpt = schema.get("properties", {}).get(ref_field, {})

    try:
        search_type = SearchType[ref_entity_type.upper()]
    except KeyError:
        raise ValueError(f"Invalid ref_entity_type: {ref_entity_type}")

    OutputModel = build_suggestion_model(schema, [ref_field])

    user_prompt = (
        f"Manage the '{ref_field}' reference field for this {ctx.entity_name}.\n\n"
        f"CURRENT ENTITY DATA:\n{json.dumps(ctx.entity_data, indent=2, default=str)}\n\n"
        f"CURRENT VALUE OF '{ref_field}':\n{json.dumps(current_ref, indent=2, default=str)}\n\n"
        f"SCHEMA FOR '{ref_field}':\n{json.dumps(schema_excerpt, indent=2, default=str)}\n\n"
    )
    if prompt:
        user_prompt += f"ADDITIONAL GUIDANCE: {prompt}\n\n"
    user_prompt += (
        "Use the search tool to find relevant entities, then return a result with:\n"
        "- 'data': the mutation payload (e.g. {'addItems': ['id1'], 'removeItems': ['id2']})\n"
        "- 'suggestions': one FieldSuggestion per operation with field name, value, confidence, reasoning"
    )

    agent = Agent(
        llm_agent(),
        output_type=OutputModel,
        system_prompt=_SYSTEM_PROMPT,
        toolsets=[search_fixed_type_toolset(search_type)],
    )

    result = agent.run_sync(user_prompt)
    typed = cast(Any, result.output)
    suggestions: list[FieldSuggestion] = typed.suggestions
    data = {k: v for k, v in typed.data.model_dump().items() if v is not None}

    def _no_ref_found(d: dict[str, Any]) -> bool:
        if not d:
            return True
        return all(isinstance(v, list) and len(v) == 0 for v in d.values())

    create_before_ref = None
    if _no_ref_found(data) and create_prompt:
        create_before_ref = {
            "entity_type": ref_entity_type,
            "prompt": create_prompt,
        }

    return SuggestResult(
        entity_name=ctx.entity_name,
        entity_id=ctx.entity_id,
        data=data if not _no_ref_found(data) else None,
        suggestions=suggestions,
        create_before_ref=create_before_ref,
    ).model_dump()
