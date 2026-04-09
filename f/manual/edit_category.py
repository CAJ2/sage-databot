# requirements: project

import json
from typing import Any

import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(
    category_id: str,
    user_id: str,
    name: dict[str, Any] | None = None,
    desc_short: dict[str, Any] | None = None,
    desc: dict[str, Any] | None = None,
    image_url: str | None = None,
) -> dict[str, Any]:
    """
    Edits the column fields of an existing category (name, desc_short, desc, image_url).
    Only the fields explicitly provided are updated. Records the change in
    public.category_history with the previous values as original.
    """
    changes: dict[str, Any] = {}
    if name is not None:
        changes["name"] = name
    if desc_short is not None:
        changes["desc_short"] = desc_short
    if desc is not None:
        changes["desc"] = desc
    if image_url is not None:
        changes["image_url"] = image_url

    if not changes:
        raise ValueError("At least one field must be provided to update.")

    engine = create_sql_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text(
                'SELECT name, desc_short, "desc", image_url FROM public.categories WHERE id = :id'
            ),
            {"id": category_id},
        ).fetchone()

    if row is None:
        raise ValueError(f"Category {category_id!r} not found.")

    original = {
        "name": row[0],
        "desc_short": row[1],
        "desc": row[2],
        "image_url": row[3],
    }

    # `desc` is a reserved SQL keyword and must be double-quoted
    QUOTED_COLS = {"desc"}

    set_clauses = ", ".join(
        f'"{col}" = :{col}' if col in QUOTED_COLS else f"{col} = :{col}"
        for col in changes
    )
    params: dict[str, Any] = {"id": category_id}
    for col, val in changes.items():
        params[col] = json.dumps(val) if isinstance(val, dict) else val

    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE public.categories SET {set_clauses}, updated_at = NOW() WHERE id = :id"  # noqa: S608
            ),
            params,
        )

        conn.execute(
            text(
                """
                INSERT INTO public.category_history (category_id, datetime, user_id, original, changes)
                VALUES (:category_id, NOW(), :user_id, :original, :changes)
                """
            ),
            {
                "category_id": category_id,
                "user_id": user_id,
                "original": json.dumps(original),
                "changes": json.dumps(changes),
            },
        )

    print(f"Updated category {category_id!r}: {list(changes)}")
    wmill.run_script_by_path_async(
        "f/search/categories/index_categories", args={"keys": [category_id]}
    )
    return {"id": category_id}
