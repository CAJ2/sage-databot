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
    Fetches rich context for a Component: its material and usage in variants.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_schema: Any = None
    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}
    result = None

    entity_data, result = fetch_context_entity(
        entity_id=entity_id,
        entity_name="Component",
        fetch_fn=client.get_component_for_review,
        result_attr="component",
    )
    if result and result.component and result.component.sources:
        source_contexts = [
            s.source.content["context"]
            for s in (result.component.sources.nodes or [])
            if s.source.content and s.source.content.get("context")
        ]
        if source_contexts:
            related_data["source_contexts"] = source_contexts

    entity_schema = fetch_context_schema(
        entity_name="Component",
        schema_mode=schema_mode,
        fetch_fn=client.get_component_schema,
        schema_attr="component_schema",
    )

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Component, consider:\n"
            "- Whether the component name/description matches the underlying material.\n"
            "- Whether quantity and unit values are physically reasonable.\n"
            "- Components represent material inputs used by variants (e.g. 'Steel frame: 2 kg').\n"
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
            "When suggesting values for a Component, consider:\n"
            "- The name should identify the specific material component.\n"
            "- Quantities should reflect realistic material usage for the product.\n"
            "- Units should be standard physical units (kg, g, L, m, etc.).\n"
            "- If source_contexts are provided in related_data, use them as primary factual references.\n"
            "- The json_schema in related_data defines the valid structure and constraints for this entity's fields.\n"
            f"- Use the linked material and variant context as reference.{fields_hint}"
        )

    return EntityContext(
        entity_name="Component",
        entity_id=entity_id,
        entity_schema=entity_schema,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
