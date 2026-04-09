# requirements: project

import json
from typing import Any, cast

from pydantic_ai import Agent

from f.agents.toolsets import search_multi_type_toolset
from f.changes.ai_shared import (
    FieldSuggestion,
    SuggestResult,
    build_suggestion_model,
)
from f.context.context_types import EntityContext
from f.utils.api import api_connect
from f.utils.general import llm_agent, llm_model_settings

_SYSTEM_PROMPT = (
    "You are a data entry expert for a product and sustainability database. "
    "Given a natural language prompt describing a new entity, use the search tool "
    "to look up related entities (categories, orgs, items, etc.) by name to find their IDs, "
    "then suggest all required and relevant field values for creating the new entity. "
    "The 'data' field in your output should be a complete, schema-compliant create mutation payload. "
    "Each field in 'suggestions' should correspond to one create schema field with a value, "
    "confidence (0.0–1.0), and concise reasoning."
)


def _fetch_related_entity(
    client: Any, entity_type: str, entity_id: str
) -> dict[str, Any] | None:
    try:
        if entity_type == "Variant":
            r = client.get_variant_for_review(id=entity_id)
            return r.variant.model_dump(by_alias=False) if r.variant else None
        elif entity_type == "Item":
            r = client.get_item_for_review(id=entity_id)
            return r.item.model_dump(by_alias=False) if r.item else None
        elif entity_type == "Component":
            r = client.get_component_for_review(id=entity_id)
            return r.component.model_dump(by_alias=False) if r.component else None
        elif entity_type == "Process":
            r = client.get_process_for_review(id=entity_id)
            return r.process.model_dump(by_alias=False) if r.process else None
        elif entity_type == "Category":
            r = client.get_category_for_review(id=entity_id)
            return r.category.model_dump(by_alias=False) if r.category else None
        elif entity_type == "Place":
            r = client.get_place_for_review(id=entity_id)
            return r.place.model_dump(by_alias=False) if r.place else None
        elif entity_type == "Material":
            r = client.get_material_for_review(id=entity_id)
            return r.material.model_dump(by_alias=False) if r.material else None
    except Exception as e:
        print(f"Could not fetch {entity_type} {entity_id}: {e}")
    return None


def main(
    entity_context: dict[str, Any],
    prompt: str,
    related_entity_ids: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """
    AI agent for creating a new entity from a natural language prompt.
    Accepts a pre-built entity_context (from a context script) containing the create
    schema, then uses a search tool to find related entities and suggests all field values.
    """
    ctx = EntityContext.model_validate(entity_context)
    entity_name = ctx.entity_name
    create_schema = ctx.entity_schema or {}

    target_fields = list(create_schema.get("properties", {}).keys())
    OutputModel = build_suggestion_model(create_schema, target_fields)

    context_str = ""
    if ctx.entity_data or ctx.related_data:
        context_str = (
            "\nCONTEXT DATA:\n"
            + json.dumps(
                {"entity": ctx.entity_data, "related": ctx.related_data},
                indent=2,
                default=str,
            )
            + "\n\n"
        )

    related_str = ""
    if related_entity_ids:
        client, _ = api_connect()
        fetched = []
        for ref in related_entity_ids:
            data = _fetch_related_entity(client, ref["entity_type"], ref["entity_id"])
            if data:
                fetched.append({"entity_type": ref["entity_type"], "data": data})
        if fetched:
            related_str = (
                "\nRELATED ENTITIES (use as context for the new entity):\n"
                + json.dumps(fetched, indent=2, default=str)
                + "\n\n"
            )

    hints_str = (
        f"\nMODEL-SPECIFIC GUIDANCE:\n{ctx.prompt_hints}\n\n"
        if ctx.prompt_hints
        else ""
    )

    user_prompt = (
        f"Create a new {entity_name} based on the following description:\n\n"
        f"{prompt}\n\n"
        # f"CREATE SCHEMA:\n{json.dumps(create_schema, indent=2, default=str)}\n\n"
        f"{context_str}"
        f"{related_str}"
        f"{hints_str}"
        "Use the search tool to find IDs for any referenced entities (categories, orgs, items, etc.). "
        f"Return a complete 'data' payload ready for the create mutation, and one FieldSuggestion per field."
    )

    agent = Agent(
        llm_agent(),
        output_type=OutputModel,
        system_prompt=_SYSTEM_PROMPT,
        toolsets=[search_multi_type_toolset()],
    )

    print("--- AI AGENT ---")
    result = agent.run_sync(
        user_prompt,
        model_settings=llm_model_settings(),
    )
    print("--- AI AGENT DONE ---")
    typed = cast(Any, result.output)
    suggestions: list[FieldSuggestion] = typed.suggestions
    data = {k: v for k, v in typed.data.model_dump().items() if v is not None}

    return SuggestResult(
        entity_name=entity_name,
        entity_id=None,
        data=data,
        suggestions=suggestions,
    ).model_dump()
