# requirements: project

import json

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models import Model


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
    context: dict,
    model: Model,
    prompt_hints: str = "",
) -> EditAnalysis:
    """
    Runs AI analysis on a single edit and returns a typed EditAnalysis result.
    prompt_hints is optional model-specific guidance injected into the prompt.
    """
    context_str = json.dumps(context, indent=2, default=str)
    hints_section = f"\nMODEL-SPECIFIC GUIDANCE:\n{prompt_hints}\n" if prompt_hints else ""
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
