# requirements: project

import json
from typing import Any, cast

from google.genai.types import ThinkingLevel
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModelSettings

from f.agents.toolsets import search_multi_type_toolset
from f.changes.ai_shared import (
    FieldSuggestion,
    SuggestResult,
    build_suggestion_model,
)
from f.context.context_types import EntityContext
from f.utils.general import llm_agent

_SYSTEM_PROMPT = (
    "You are a data entry expert for a product and sustainability database. "
    "Given a natural language prompt describing a new entity, use the search tool "
    "to look up related entities (categories, orgs, items, etc.) by name to find their IDs, "
    "then suggest all required and relevant field values for creating the new entity. "
    "The 'data' field in your output should be a complete, schema-compliant create mutation payload. "
    "Each field in 'suggestions' should correspond to one create schema field with a value, "
    "confidence (0.0–1.0), and concise reasoning."
)


def main(entity_context: dict[str, Any], prompt: str) -> dict[str, Any]:
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

    hints_str = (
        f"\nMODEL-SPECIFIC GUIDANCE:\n{ctx.prompt_hints}\n\n"
        if ctx.prompt_hints
        else ""
    )

    user_prompt = (
        f"Create a new {entity_name} based on the following description:\n\n"
        f"{prompt}\n\n"
        f"CREATE SCHEMA:\n{json.dumps(create_schema, indent=2, default=str)}\n\n"
        f"{context_str}"
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
        model_settings=GoogleModelSettings(
            google_thinking_config={"thinking_level": ThinkingLevel.LOW}
        ),
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
