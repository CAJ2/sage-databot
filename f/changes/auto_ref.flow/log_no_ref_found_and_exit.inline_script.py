def main(entity_name: str, ref_field: str, ref_entity_type: str) -> dict[str, object]:
    msg = (
        f"No existing {ref_entity_type} found for {entity_name}.{ref_field}. "
        "Provide create_prompt to enable auto-creation."
    )
    print(msg)
    return {"skipped": True, "reason": msg}
