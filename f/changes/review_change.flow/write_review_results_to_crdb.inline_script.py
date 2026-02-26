# requirements: project

import os
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from f.changes.ai_review import EditAnalysis, ReviewSummary
from f.db.sage.model import Change
from f.graphql.api_client.enums import ChangeStatus
from f.graphql.api_client.input_types import UpdateChangeInput
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine


def main(change_id: str, edit_analyses: list[EditAnalysis]) -> ReviewSummary:
    """
    Collects all per-edit EditAnalysis results, determines the overall verdict,
    writes review metadata to the Change record in CRDB via ORM, and updates
    the Change status via the GraphQL API.
    """
    # Windmill deserializes Pydantic models from JSON; re-validate to ensure type safety
    results = [
        EditAnalysis.model_validate(r) if isinstance(r, dict) else r
        for r in (edit_analyses or [])
        if r is not None
    ]

    if not results:
        raise ValueError(
            f"No edit analyses received for change {change_id}. "
            "The for-loop produced no results — check that edits exist and analyzers succeeded."
        )

    overall_approved = all(r.approved for r in results)
    new_status = ChangeStatus.APPROVED if overall_approved else ChangeStatus.REJECTED

    run_id = os.environ.get("WM_JOB_ID", "unknown")
    reviewed_at = datetime.now(timezone.utc).isoformat()

    metadata = {
        "review": {
            "reviewed_at": reviewed_at,
            "run_id": run_id,
            "overall_approved": overall_approved,
            "edit_count": len(results),
            "approved_count": sum(1 for r in results if r.approved),
            "rejected_count": sum(1 for r in results if not r.approved),
            "edits": [r.model_dump() for r in results],
        }
    }

    # Write metadata and status via ORM
    engine = create_sql_engine()
    with Session(engine) as session:
        change = session.get(Change, change_id)
        if change is None:
            raise ValueError(f"Change {change_id} not found in CRDB.")
        change.status = new_status.value
        change.metadata_ = metadata
        change.updated_at = datetime.now(timezone.utc)
        session.commit()

    print(f"Updated change {change_id}: status={new_status.value}, {len(results)} edits analyzed")

    # Also update status via GraphQL API for consistency
    try:
        client, _ = api_connect()
        client.update_change_status(
            input=UpdateChangeInput(id=change_id, status=new_status)
        )
    except Exception as e:
        print(f"Warning: GraphQL status update failed (CRDB already updated): {e}")

    return ReviewSummary(
        change_id=change_id,
        status=new_status.value,
        overall_approved=overall_approved,
        edit_count=len(results),
        approved_count=sum(1 for r in results if r.approved),
        rejected_count=sum(1 for r in results if not r.approved),
        reviewed_at=reviewed_at,
        run_id=run_id,
    )
