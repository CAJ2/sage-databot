# requirements: project

from typing import Any

from f.context.context_helpers import fetch_context_entity, fetch_context_schema
from f.context.context_types import ContextMode, EntityContext, SchemaMode
from f.utils.api import api_connect


def main(
    entity_id: str | None = None,
    mode: ContextMode = "review",
    schema_mode: SchemaMode = "update",
    target_fields: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetches rich context for an Item: its categories and linked variants.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_schema: Any = None
    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}

    entity_data, _ = fetch_context_entity(
        entity_id=entity_id,
        entity_name="Item",
        fetch_fn=client.get_item_for_review,
        result_attr="item",
    )
    entity_schema = fetch_context_schema(
        entity_name="Item",
        schema_mode=schema_mode,
        fetch_fn=client.get_item_schema,
        schema_attr="item_schema",
    )

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to an Item, consider:\n"
            "- Whether the name/description clearly identifies the generic product type.\n"
            "- Whether assigned categories are appropriate for the product.\n"
            "- Items are generic types (e.g. 'Oat Milk'); Variants are specific products.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields."
        )
    else:
        fields_hint = (
            f" Focus especially on: {', '.join(target_fields)}."
            if target_fields
            else ""
        )
        prompt_hints = (
            "When suggesting values for an Item, consider:\n"
            "- The name should represent a generic product type, not a brand.\n"
            "- The description should describe the category of products this Item covers.\n"
            "- Categories should reflect the item's place in a product taxonomy.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields.\n"
            f"- Use existing variants and categories as context clues.{fields_hint}"
        )

    return EntityContext(
        entity_name="Item",
        entity_id=entity_id,
        entity_schema=entity_schema,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
