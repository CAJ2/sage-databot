# requirements: project

from f.context.context_types import ContextMode, EntityContext
from f.utils.api import api_connect


def main(
    entity_id: str,
    mode: ContextMode = "review",
    target_fields: list[str] | None = None,
) -> EntityContext:
    """
    Fetches rich context for an Item: its categories and linked variants.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict = {}
    related_data: dict = {}

    try:
        result = client.get_item_for_review(id=entity_id)
        if result.item:
            entity_data = result.item.model_dump(by_alias=False)
    except Exception as e:
        print(f"Could not fetch Item {entity_id}: {e}")

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to an Item, consider:\n"
            "- Whether the name/description clearly identifies the generic product type.\n"
            "- Whether assigned categories are appropriate for the product.\n"
            "- Items are generic types (e.g. 'Oat Milk'); Variants are specific products."
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
            f"- Use existing variants and categories as context clues.{fields_hint}"
        )

    return EntityContext(
        entity_name="Item",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    )
