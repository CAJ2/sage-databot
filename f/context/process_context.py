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
    Fetches rich context for a Process: its material, place, org, region, and variant.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict[str, Any] = {}
    related_data: dict[str, Any] = {}
    result = None

    try:
        result = client.get_process_for_review(id=entity_id)
        if result.process:
            entity_data = result.process.model_dump(by_alias=False)
    except Exception as e:
        print(f"Could not fetch Process {entity_id}: {e}")

    # Fetch linked material for additional context
    material_id = entity_data.get("material", {}) or {}
    if isinstance(material_id, dict):
        material_id = material_id.get("id")
    if material_id:
        try:
            mat_result = client.get_material_for_review(id=material_id)
            if mat_result.material:
                related_data["material"] = mat_result.material.model_dump(
                    by_alias=False
                )
        except Exception as e:
            print(f"Could not fetch Material {material_id}: {e}")

    if result and result.process and result.process.sources:
        source_contexts = [
            s.source.content["context"]
            for s in (result.process.sources.nodes or [])
            if s.source.content and s.source.content.get("context")
        ]
        if source_contexts:
            related_data["source_contexts"] = source_contexts

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Process, consider:\n"
            "- Whether the material being processed is consistent with the process intent.\n"
            "- Whether the place and org are plausible for this type of process.\n"
            "- Whether efficiency values (efficiency, equivalency, valueRatio) are physically realistic.\n"
            "- Process intents include: production, transformation, transport, end-of-life, etc.\n"
            "- If source_contexts are provided in related_data, use them as primary factual references."
        )
    else:
        fields_hint = (
            f" Focus especially on: {', '.join(target_fields)}."
            if target_fields
            else ""
        )
        prompt_hints = (
            "When suggesting values for a Process, consider:\n"
            "- The name should describe what the process does (e.g. 'Cold-pressed extraction').\n"
            "- The intent field categorizes the process type (production, transport, etc.).\n"
            "- Efficiency values should be based on realistic industry benchmarks.\n"
            "- If source_contexts are provided in related_data, use them as primary factual references.\n"
            f"- Use the linked material, place, and org as context for plausible values.{fields_hint}"
        )

    return EntityContext(
        entity_name="Process",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    ).model_dump()
