# requirements: project

import json
from typing import Any, Optional, cast

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent
from pydantic_ai.models import Model
from pydantic_ai.usage import UsageLimits

from f.context.context_types import EntityContext
from f.utils.general import llm_agent


class FieldSuggestion(BaseModel):
    """Field suggestion metadata."""

    field: str
    confidence: float = Field(ge=0.0, le=1.0)  # 0.0 – 1.0
    reasoning: str


class SuggestResult(BaseModel):
    """All field suggestions for a single entity, returned by the auto-suggest flow."""

    entity_name: str
    entity_id: str | None
    data: dict[str, Any] | None = None
    suggestions: list[FieldSuggestion]


class LLMSuggestOutput(BaseModel):
    """Structured output from the AI suggestion agent (fallback when no JSON schema available)."""

    suggestions: list[FieldSuggestion]


def _resolve_python_type(prop: dict[str, Any], defs: dict[str, Any]) -> Any:
    """Map a JSON Schema property dict to a Python type for create_model()."""
    if "$ref" in prop:
        ref_name = prop["$ref"].split("/")[-1]
        return _resolve_python_type(defs.get(ref_name, {}), defs)
    # Handle anyOf (e.g. nullable fields: anyOf: [{type: string}, {type: null}])
    if "anyOf" in prop:
        non_null = [t for t in prop["anyOf"] if t.get("type") != "null"]
        if len(non_null) == 1:
            return _resolve_python_type(non_null[0], defs)
        return Any
    t = prop.get("type")
    if t == "string":
        return str
    if t == "integer":
        return int
    if t == "number":
        return float
    if t == "boolean":
        return bool
    return Any  # arrays, objects, oneOf → untyped fallback


def build_suggestion_model(
    schema: dict[str, Any], target_fields: list[str]
) -> type[BaseModel]:
    """
    Builds a dynamic Pydantic model for pydantic-ai's output_type.
    Shape: {data: {field: <typed_value>, ...}, suggestions: [FieldSuggestion, ...]}
    The LLM fills both independently: data is mutation-ready, suggestions carry confidence/reasoning.
    """
    defs: dict[str, Any] = schema.get("$defs", {})
    properties: dict[str, Any] = schema.get("properties", {})
    data_fields: dict[str, Any] = {}
    for field in target_fields:
        prop: dict[str, Any] = properties.get(field, {})
        python_type = _resolve_python_type(prop, defs)
        data_fields[field] = (Optional[python_type], None)
    DataModel = create_model("DataModel", **data_fields)
    return create_model(
        "SuggestionOutput",
        data=(DataModel, ...),
        suggestions=(list[FieldSuggestion], ...),
    )


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
    model: Model,
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
    suggestions = typed.suggestions
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
