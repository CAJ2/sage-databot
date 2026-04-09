# requirements: project

import json
from typing import Any

import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(
    material_id: str,
    user_id: str,
    name: dict[str, Any] | None = None,
    desc: dict[str, Any] | None = None,
    source: dict[str, Any] | None = None,
    technical: bool | None = None,
    shape: str | None = None,
) -> dict[str, Any]:
    """
    Edits the column fields of an existing material (name, desc, source, technical, shape).
    Only the fields explicitly provided are updated. Records the change in
    public.material_history with the previous values as original.
    """
    changes: dict[str, Any] = {}
    if name is not None:
        changes["name"] = name
    if desc is not None:
        changes["desc"] = desc
    if source is not None:
        changes["source"] = source
    if technical is not None:
        changes["technical"] = technical
    if shape is not None:
        changes["shape"] = shape

    if not changes:
        raise ValueError("At least one field must be provided to update.")

    engine = create_sql_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text(
                'SELECT name, "desc", source, technical, shape FROM public.materials WHERE id = :id'
            ),
            {"id": material_id},
        ).fetchone()

    if row is None:
        raise ValueError(f"Material {material_id!r} not found.")

    original = {
        "name": row[0],
        "desc": row[1],
        "source": row[2],
        "technical": row[3],
        "shape": row[4],
    }

    # `desc` is a reserved SQL keyword and must be double-quoted
    QUOTED_COLS = {"desc"}

    set_clauses = ", ".join(
        f'"{col}" = :{col}' if col in QUOTED_COLS else f"{col} = :{col}"
        for col in changes
    )
    params: dict[str, Any] = {"id": material_id}
    for col, val in changes.items():
        params[col] = json.dumps(val) if isinstance(val, dict) else val

    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE public.materials SET {set_clauses}, updated_at = NOW() WHERE id = :id"  # noqa: S608
            ),
            params,
        )

        conn.execute(
            text(
                """
                INSERT INTO public.material_history (material_id, datetime, user_id, original, changes)
                VALUES (:material_id, NOW(), :user_id, :original, :changes)
                """
            ),
            {
                "material_id": material_id,
                "user_id": user_id,
                "original": json.dumps(original),
                "changes": json.dumps(changes),
            },
        )

    print(f"Updated material {material_id!r}: {list(changes)}")
    wmill.run_script_by_path_async(
        "f/search/materials/index_materials", args={"keys": [material_id]}
    )
    return {"id": material_id}
