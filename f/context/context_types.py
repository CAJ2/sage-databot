from typing import Literal
from pydantic import BaseModel


ContextMode = Literal["review", "suggest"]


class EntityContext(BaseModel):
    """
    Structured context produced by a model-specific context script.
    Consumed by both the review and auto-suggest flows.
    """

    entity_name: str
    entity_id: str | None = None
    entity_data: dict
    related_data: dict
    # Model-specific guidance for the AI, tailored to the requested mode.
    # In "review" mode: explains what makes changes to this entity valid/invalid.
    # In "suggest" mode: describes what good field values look like given the context.
    prompt_hints: str
