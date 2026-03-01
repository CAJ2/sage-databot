# requirements: project

from f.context.context_types import ContextMode, EntityContext
from f.utils.api import api_connect


def main(
    entity_id: str,
    mode: ContextMode = "review",
    target_fields: list[str] | None = None,
) -> EntityContext:
    """
    Fetches rich context for a Category: its parent, children, and associated items.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict = {}
    related_data: dict = {}

    try:
        result = client.get_category_for_review(id=entity_id)
        if result.category:
            entity_data = result.category.model_dump(by_alias=False)
    except Exception as e:
        print(f"Could not fetch Category {entity_id}: {e}")

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
            f"- Follow existing naming conventions in the taxonomy.{fields_hint}"
        )

    return EntityContext(
        entity_name="Category",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    )
