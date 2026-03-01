# requirements: project

import json

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models import Model

from f.context.context_types import EntityContext


class FieldSuggestion(BaseModel):
    """A single AI-generated field value suggestion."""

    field: str
    suggested_value: str
    confidence: float = Field(ge=0.0, le=1.0)  # 0.0 – 1.0
    reasoning: str


class SuggestResult(BaseModel):
    """All field suggestions for a single entity, returned by the auto-suggest flow."""

    entity_name: str
    entity_id: str | None
    suggestions: list[FieldSuggestion]


class LLMSuggestOutput(BaseModel):
    """Structured output from the AI suggestion agent."""

    suggestions: list[FieldSuggestion]


def build_suggest_agent(model: Model) -> Agent[None, LLMSuggestOutput]:
    return Agent(
        model,
        output_type=LLMSuggestOutput,
        system_prompt=(
            "You are a data enrichment expert for a product and sustainability database. "
            "Given an entity's current data and related context, suggest accurate and appropriate "
            "values for the requested fields. "
            "Be specific and grounded in the provided context. "
            "Express your confidence as a float between 0.0 (very uncertain) and 1.0 (very confident). "
            "Provide concise reasoning for each suggestion."
        ),
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

    agent = build_suggest_agent(model)
    result = agent.run_sync(prompt)
    output = result.output

    return SuggestResult(
        entity_name=context.entity_name,
        entity_id=context.entity_id,
        suggestions=output.suggestions,
    )
