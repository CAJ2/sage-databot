# requirements: project

from pydantic import BaseModel

from f.utils.api import api_connect


class ChangeEditInput(BaseModel):
    """Input data for a single edit, passed to each analyzer script via the for-loop."""
    change_id: str
    edit_id: str
    entity_name: str
    create_changes: dict | None = None
    update_changes: dict | None = None
    proposed_id: str | None = None
    original_id: str | None = None


def main(change_id: str) -> list[ChangeEditInput]:
    """
    Fetches a Change and all its Edit nodes from the GraphQL API using the
    typed client. Returns a flat list of ChangeEditInput for the for-loop iterator.
    """
    client, _ = api_connect()
    result = client.get_change_for_review(id=change_id)

    if result.change is None:
        raise ValueError(f"Change not found: {change_id}")

    edits = result.change.edits.nodes or []
    if not edits:
        print(f"Change {change_id} has no edits.")
        return []

    inputs: list[ChangeEditInput] = []
    for edit in edits:
        proposed_id = edit.changes.id if edit.changes else None
        original_id = edit.original.id if edit.original else None
        inputs.append(
            ChangeEditInput(
                change_id=change_id,
                edit_id=edit.id or "",
                entity_name=edit.entity_name,
                create_changes=edit.create_changes,
                update_changes=edit.update_changes,
                proposed_id=proposed_id,
                original_id=original_id,
            )
        )

    print(f"Found {len(inputs)} edits in change {change_id}")
    return inputs
