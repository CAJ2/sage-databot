# requirements: project

from typing import Any

from f.context.context_types import ContextMode, EntityContext
from f.utils.api import api_connect


def main(
    entity_id: str,
    mode: ContextMode = "review",
    target_fields: list[str] | None = None,
) -> dict[str, Any]:
    """
    Fetches rich context for a Component: its material and usage in variants.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}

    try:
        result = client.get_component_for_review(id=entity_id)
        if result.component:
            entity_data = result.component.model_dump(by_alias=False)
            if result.component.sources:
                source_contexts = [
                    s.source.content["context"]
                    for s in (result.component.sources.nodes or [])
                    if s.source.content and s.source.content.get("context")
                ]
                if source_contexts:
                    related_data["source_contexts"] = source_contexts
    except Exception as e:
        raise ValueError(f"Could not fetch Component {entity_id}: {e}")
    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Component, consider:\n"
            "- Whether the component name/description matches the underlying material.\n"
            "- Whether quantity and unit values are physically reasonable.\n"
            "- Components represent material inputs used by variants (e.g. 'Steel frame: 2 kg').\n"
            "- If source_contexts are provided in related_data, use them as primary factual references."
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
            f"- Use the linked material and variant context as reference.{fields_hint}"
        )

    return EntityContext(
        entity_name="Component",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
