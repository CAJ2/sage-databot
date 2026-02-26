# requirements: project

from f.context.context_types import ContextMode, EntityContext
from f.utils.api import api_connect


def main(
    entity_id: str,
    mode: ContextMode = "review",
    target_fields: list[str] | None = None,
) -> EntityContext:
    """
    Fetches rich context for a Place: its address, location, and linked org.
    Used by both the review and auto-suggest flows.
    """
    client, _ = api_connect()

    entity_data: dict = {}
    related_data: dict = {}

    try:
        result = client.get_place_for_review(id=entity_id)
        if result.place:
            entity_data = result.place.model_dump(by_alias=False)
    except Exception as e:
        print(f"Could not fetch Place {entity_id}: {e}")

    if mode == "review":
        prompt_hints = (
            "When reviewing changes to a Place, consider:\n"
            "- Whether the address and geographic coordinates are internally consistent.\n"
            "- Whether the linked org plausibly operates at this location.\n"
            "- Whether the place type (factory, warehouse, farm, etc.) matches the description."
        )
    else:
        fields_hint = f" Focus especially on: {', '.join(target_fields)}." if target_fields else ""
        prompt_hints = (
            "When suggesting values for a Place, consider:\n"
            "- The name should identify the facility or location clearly.\n"
            "- Address fields should follow standard formats for the country.\n"
            "- Use the linked org's industry and existing places as context.\n"
            f"- Geographic coordinates should match the address.{fields_hint}"
        )

    return EntityContext(
        entity_name="Place",
        entity_id=entity_id,
        entity_data=entity_data,
        related_data=related_data,
        prompt_hints=prompt_hints,
    )
