# requirements: project

import json
from typing import Any, Optional

from pydantic import BaseModel, Field, create_model
from pydantic_ai import Agent
from pydantic_ai.models import Model


# --- Suggestion types ---


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
    create_before_ref: dict[str, Any] | None = None
    """When set: {"entity_type": str, "prompt": str}.
    Signals auto_ref.flow to run auto_create.flow first, then link the created entity."""


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
    if t == "array":
        items = prop.get("items", {})
        item_type = _resolve_python_type(items, defs) if items else Any
        return list[item_type]
    if t == "object":
        return dict[str, Any]
    if "oneOf" in prop:
        non_null = [s for s in prop["oneOf"] if s.get("type") != "null"]
        if len(non_null) == 1:
            return _resolve_python_type(non_null[0], defs)
        return Any
    return Any


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


# --- Review types ---


class FieldAnalysis(BaseModel):
    field: str
    approved: bool
    note: str = ""


class LLMEditVerdict(BaseModel):
    """Structured output returned by the AI review agent."""

    approved: bool
    reasoning: str
    field_analyses: list[FieldAnalysis] = []


class EditAnalysis(BaseModel):
    """Result of analyzing a single Edit within a Change. Returned by each analyzer script."""

    edit_id: str
    entity_name: str
    entity_id: str | None = None
    approved: bool
    reasoning: str
    field_analyses: list[FieldAnalysis] = []


class ReviewSummary(BaseModel):
    """Final review outcome written to the Change record. Returned by the write-results script."""

    change_id: str
    status: str  # APPROVED or REJECTED
    overall_approved: bool
    edit_count: int
    approved_count: int
    rejected_count: int
    reviewed_at: str
    run_id: str


def build_analysis_agent(model: Model) -> Agent[None, LLMEditVerdict]:
    return Agent(
        model,
        output_type=LLMEditVerdict,
        system_prompt=(
            "You are a data quality expert reviewing proposed changes to a product and sustainability database. "
            "Your job is to assess whether each proposed change is reasonable, accurate, and coherent. "
            "For relational changes (e.g. connecting a Variant to an Item), verify that the relationship makes sense. "
            "For field updates, check that the values are appropriate and consistent with the existing context. "
            "Be concise but specific in your reasoning. "
            "Only reject changes that are clearly wrong, inconsistent, or suspicious."
        ),
    )


def analyze_edit(
    edit_id: str,
    entity_name: str,
    entity_id: str | None,
    change_description: str,
    context: dict[str, Any],
    model: Model,
    prompt_hints: str = "",
) -> EditAnalysis:
    """
    Runs AI analysis on a single edit and returns a typed EditAnalysis result.
    prompt_hints is optional model-specific guidance injected into the prompt.
    """
    context_str = json.dumps(context, indent=2, default=str)
    hints_section = (
        f"\nMODEL-SPECIFIC GUIDANCE:\n{prompt_hints}\n" if prompt_hints else ""
    )
    prompt = (
        f"Review the following proposed change to a {entity_name} record.\n\n"
        f"CHANGE DESCRIPTION:\n{change_description}\n\n"
        f"CONTEXT:\n{context_str}\n"
        f"{hints_section}\n"
        "Is this change reasonable? Evaluate each modified field and any relational links. "
        "Return your verdict (approved: true/false), concise reasoning, and per-field analysis."
    )

    agent = build_analysis_agent(model)
    result = agent.run_sync(prompt)
    output = result.output

    return EditAnalysis(
        edit_id=edit_id,
        entity_name=entity_name,
        entity_id=entity_id,
        approved=output.approved,
        reasoning=output.reasoning,
        field_analyses=output.field_analyses,
    )
