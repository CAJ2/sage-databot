# requirements: project

from typing import Any

from f.context.context_types import ContextMode, EntityContext, SchemaMode


def main(
    entity_id: str | None = None,
    mode: ContextMode = "review",
    schema_mode: SchemaMode = "update",  # noqa: ARG001  # pyright: ignore[reportUnusedParameter]
    target_fields: list[str] | None = None,
) -> dict[str, Any]:
    """
    Generic fallback context script for model types without a dedicated script
    (e.g. Material, Region). Returns minimal context with no API fetch.
    """
    if mode == "review":
        prompt_hints = (
            "Review the proposed change carefully. "
            "Check that all field values are reasonable, consistent, and accurate."
        )
    else:
        fields_hint = f" Focus on: {', '.join(target_fields)}." if target_fields else ""
        prompt_hints = (
            "Suggest appropriate values based on the entity context provided. "
            f"Values should be accurate, consistent, and follow existing conventions.{fields_hint}"
        )

    return EntityContext(
        entity_name="Unknown",
        entity_id=entity_id,
        entity_data={},
        related_data={},
        prompt_hints=prompt_hints,
    ).model_dump()
