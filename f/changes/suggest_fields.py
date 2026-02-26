# requirements: project

from f.changes.ai_suggest import SuggestResult, suggest_fields
from f.context.context_types import EntityContext
from f.utils.general import llm_agent


def main(
    entity_context: dict,
    target_fields: list[str],
) -> SuggestResult:
    """
    Generic AI field suggestion using pre-fetched EntityContext.
    Accepts entity_context as a plain dict (Windmill serializes Pydantic models
    across flow steps) and re-validates it into EntityContext.
    """
    ctx = EntityContext.model_validate(entity_context)
    model = llm_agent()
    return suggest_fields(ctx, target_fields, model)
