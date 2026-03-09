# requirements: project

import json
from typing import Any, cast

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from f.changes.ai_shared import (
    FieldSuggestion,
    SuggestResult,
    build_suggestion_model,
)
from f.context.context_types import EntityContext
from f.utils.general import llm_agent


_SYSTEM_PROMPT = (
    "You are a data enrichment expert for a product and sustainability database. "
    "Given an entity's current data and related context, suggest accurate and appropriate "
    "values for the requested fields. "
    "Be specific and grounded in the provided context. "
    "Express your confidence as a float between 0.0 (very uncertain) and 1.0 (very confident). "
    "Provide concise reasoning for each suggestion."
)


def suggest_fields(
    context: EntityContext,
    target_fields: list[str],
    model: Any,
) -> SuggestResult:
    """
    Runs AI suggestion for the given fields using the pre-built EntityContext.
    """
    context_str = json.dumps(
        {"entity": context.entity_data, "related": context.related_data},
        indent=2,
        default=str,
    )
    fields_list = ", ".join(f'"{f}"' for f in target_fields)
    prompt = (
        f"Given the following {context.entity_name} record and its related data, "
        f"suggest values for these fields: {fields_list}.\n\n"
        f"CURRENT DATA:\n{context_str}\n\n"
        f"MODEL-SPECIFIC GUIDANCE:\n{context.prompt_hints}\n\n"
        "For each field, provide a specific suggested value, your confidence (0–1), and brief reasoning."
    )

    raw_schema = context.entity_schema
    if raw_schema and target_fields:
        print("Building suggestion model")
        OutputModel = build_suggestion_model(raw_schema, target_fields)
    else:
        raise ValueError("No schema available for suggestion model")

    agent = Agent(model, output_type=OutputModel, system_prompt=_SYSTEM_PROMPT)
    result = agent.run_sync(
        prompt,
        usage_limits=UsageLimits(input_tokens_limit=20000, output_tokens_limit=2000),
    )
    print("Agent finished processing")
    output = result.output

    typed = cast(Any, output)
    suggestions: list[FieldSuggestion] = typed.suggestions
    data = {k: v for k, v in typed.data.model_dump().items() if v is not None}

    return SuggestResult(
        entity_name=context.entity_name,
        entity_id=context.entity_id,
        data=data,
        suggestions=suggestions,
    )


def main(
    entity_context: dict[str, Any],
    target_fields: list[str],
) -> dict[str, Any]:
    """
    Windmill entrypoint. Accepts entity_context as a plain dict (Windmill serializes
    Pydantic models across flow steps) and re-validates it into EntityContext.
    """
    ctx = EntityContext.model_validate(entity_context)
    return suggest_fields(ctx, target_fields, llm_agent()).model_dump()
