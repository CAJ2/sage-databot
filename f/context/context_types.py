from typing import Any, Literal

from pydantic import BaseModel

ContextMode = Literal["review", "suggest"]
SchemaMode = Literal["create", "update"]


class EntityContext(BaseModel):
    """
    Structured context produced by a model-specific context script.
    Consumed by both the review and auto-suggest flows.
    """

    entity_name: str
    entity_id: str | None = None
    entity_schema: Any = None
    entity_data: dict[str, Any]
    related_data: dict[str, Any]
    # Model-specific guidance for the AI, tailored to the requested mode.
    # In "review" mode: explains what makes changes to this entity valid/invalid.
    # In "suggest" mode: describes what good field values look like given the context.
    prompt_hints: str
    # If set, signals a validation failure that occurred before the LLM review step.
    # The review_edit script will short-circuit and return a rejection with this message.
    error_details: str | None = None
