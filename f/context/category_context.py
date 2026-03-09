# requirements: project

from typing import Any

from f.context.context_types import ContextMode, EntityContext, SchemaMode
from f.utils.api import api_connect


def main(
    entity_id: str | None = None,
    mode: ContextMode = "review",
    schema_mode: SchemaMode = "update",
    target_fields: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetches rich context for a Category: its parent, children, and associated items.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_schema: Any = None
    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}

    if entity_id is not None:
        try:
            result = client.get_category_for_review(id=entity_id)
            if result.category:
                entity_data = result.category.model_dump(by_alias=False)
        except Exception as e:
            print(f"Could not fetch Category {entity_id}: {e}")

    try:
        schema_result = client.get_category_schema()
        if schema_result.category_schema:
            schema_obj = (
                schema_result.category_schema.create
                if schema_mode == "create"
                else schema_result.category_schema.update
            )
            entity_schema = schema_obj.schema_ if schema_obj else None
    except Exception as e:
        raise Exception(f"Could not fetch Category schema: {e}")

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Category, consider:\n"
            "- Whether the category name/description fits its position in the taxonomy.\n"
            "- Whether parent/child relationships are semantically consistent.\n"
            "- Whether newly linked items are appropriate members of this category."
        )
    else:
        fields_hint = (
            f" Focus especially on: {', '.join(target_fields)}."
            if target_fields
            else ""
        )
        prompt_hints = (
            "When suggesting values for a Category, consider:\n"
            "- The name should be a clear, concise product category label.\n"
            "- The description should explain what products belong in this category.\n"
            "- Use parent and sibling categories to infer the appropriate level of specificity.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields.\n"
            f"- Follow existing naming conventions in the taxonomy.{fields_hint}"
        )

    return EntityContext(
        entity_name="Category",
        entity_id=entity_id,
        entity_schema=entity_schema,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
