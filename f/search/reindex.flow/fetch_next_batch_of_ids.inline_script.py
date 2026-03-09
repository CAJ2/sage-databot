# requirements: project
import wmill
from f.utils.db.crdb import create_sql_engine
from sqlalchemy import text

ALLOWED_TABLES = {
    "categories",
    "regions",
    "orgs",
    "variants",
    "components",
    "materials",
    "places",
    "items",
}


def main(table: str, batch_size: int, start_cursor: str = ""):
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Unknown table: {table}")
    cursor = wmill.get_flow_user_state("cursor") or start_cursor
    engine = create_sql_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT id FROM public.{table} WHERE id > :cursor ORDER BY id LIMIT :limit"
            ),
            {"cursor": cursor, "limit": batch_size},
        ).fetchall()
    ids = [r[0] for r in rows]
    if ids:
        wmill.set_flow_user_state("cursor", ids[-1])
    print(f"Fetched {len(ids)} ids from {table}, cursor={ids[-1] if ids else 'done'}")
    return {"ids": ids, "count": len(ids)}
