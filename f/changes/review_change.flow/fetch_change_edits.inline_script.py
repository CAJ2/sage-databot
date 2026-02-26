# requirements: project

from pydantic import BaseModel

from f.utils.api import api_connect


class ChangeEditInput(BaseModel):
    """Input data for a single edit, passed into the for-loop iterator."""
    change_id: str
    edit_id: str
    entity_name: str
    entity_id: str | None = None  # resolved from original_id or proposed_id
    create_changes: dict | None = None
    update_changes: dict | None = None


def main(change_id: str) -> list[ChangeEditInput]:
    """
    Fetches a Change and all its Edit nodes from the GraphQL API.
    Returns a flat list of ChangeEditInput for the for-loop iterator.
    Raises ValueError if the Change is not found or has no edits.
    """
    client, _ = api_connect()
    result = client.get_change_for_review(id=change_id)

    if result.change is None:
        raise ValueError(f"Change not found: {change_id}")

    edits = result.change.edits.nodes
    if not edits:
        raise ValueError(
            f"Change {change_id} has no edits. "
            "Cannot review a Change with no Edits defined."
        )

    inputs: list[ChangeEditInput] = []
    for edit in edits:
        # Prefer original (existing entity) for context; fall back to proposed (new entity)
        entity_id = (edit.original.id if edit.original else None) or (
            edit.changes.id if edit.changes else None
        )
        inputs.append(
            ChangeEditInput(
                change_id=change_id,
                edit_id=edit.id or "",
                entity_name=edit.entity_name,
                entity_id=entity_id,
                create_changes=edit.create_changes,
                update_changes=edit.update_changes,
            )
        )

    print(f"Found {len(inputs)} edits in change {change_id}")
    return inputs
