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
    Fetches rich context for a Variant: its items, components, and orgs.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_schema: Any = None
    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}
    result = None

    entity_data, result = fetch_context_entity(
        entity_id=entity_id,
        entity_name="Variant",
        fetch_fn=client.get_variant_for_review,
        result_attr="variant",
    )
    if result and result.variant and result.variant.sources:
        source_contexts = [
            s.source.content["context"]
            for s in (result.variant.sources.nodes or [])
            if s.source.content and s.source.content.get("context")
        ]
        if source_contexts:
            related_data["source_contexts"] = source_contexts

    entity_schema = fetch_context_schema(
        entity_name="Variant",
        schema_mode=schema_mode,
        fetch_fn=client.get_variant_schema,
        schema_attr="variant_schema",
    )

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Variant, consider:\n"
            "- Whether the name/description is accurate and consistent with linked Items.\n"
            "- Whether new Item links are semantically appropriate (same product category).\n"
            "- Whether component quantities and units are physically reasonable.\n"
            "- Whether the org roles (manufacturer, distributor, etc.) are plausible.\n"
            "- If source_contexts are provided in related_data, use them as primary factual references.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields."
        )
    else:
        fields_hint = (
            f" Focus especially on: {', '.join(target_fields)}."
            if target_fields
            else ""
        )
        prompt_hints = (
            "When suggesting values for a Variant, consider:\n"
            "- The name should be specific, product-level (e.g. brand + product name).\n"
            "- The description should explain what the product is and its key attributes.\n"
            "- Linked Items represent the generic product type this variant belongs to.\n"
            "- If source_contexts are provided in related_data, use them as primary factual references.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields.\n"
            f"- Use existing linked data as style/format guidance.{fields_hint}"
        )

    return EntityContext(
        entity_name="Variant",
        entity_id=entity_id,
        entity_schema=entity_schema,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
