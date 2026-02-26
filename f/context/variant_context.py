# requirements: project

from typing import Literal

from f.context.context_types import ContextMode, EntityContext
from f.utils.api import api_connect


def main(
    entity_id: str,
    mode: ContextMode = "review",
    target_fields: list[str] | None = None,
) -> EntityContext:
    """
    Fetches rich context for a Variant: its items, components, and orgs.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict = {}
    related_data: dict = {}

    try:
        result = client.get_variant_for_review(id=entity_id)
        if result.variant:
            entity_data = result.variant.model_dump(by_alias=False)
    except Exception as e:
        print(f"Could not fetch Variant {entity_id}: {e}")

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Variant, consider:\n"
            "- Whether the name/description is accurate and consistent with linked Items.\n"
            "- Whether new Item links are semantically appropriate (same product category).\n"
            "- Whether component quantities and units are physically reasonable.\n"
            "- Whether the org roles (manufacturer, distributor, etc.) are plausible."
        )
    else:
        fields_hint = f" Focus especially on: {', '.join(target_fields)}." if target_fields else ""
        prompt_hints = (
            "When suggesting values for a Variant, consider:\n"
            "- The name should be specific, product-level (e.g. brand + product name).\n"
            "- The description should explain what the product is and its key attributes.\n"
            "- Linked Items represent the generic product type this variant belongs to.\n"
            f"- Use existing linked data as style/format guidance.{fields_hint}"
        )

    return EntityContext(
        entity_name="Variant",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    )
