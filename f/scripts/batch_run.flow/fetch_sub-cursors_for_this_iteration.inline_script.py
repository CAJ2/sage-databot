# requirements: project
import wmill
from f.utils.db.crdb import create_sql_engine
from sqlalchemy import text


def main(
    sql_query: str,
    batch_size: int,
    limit: int,
    db_resource: str = "f/db_config/db_sage",
    start_cursor: str = "",
    cursor_column: str = "id",
):
    cursor = wmill.get_flow_user_state("cursor") or start_cursor
    engine = create_sql_engine(resource=db_resource)
    total = batch_size * limit
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT {cursor_column} FROM ({sql_query}) AS q WHERE {cursor_column} > :cursor ORDER BY {cursor_column} LIMIT :total"
            ),
            {"cursor": cursor, "total": total},
        ).fetchall()
    if not rows:
        return {"sub_cursors": [], "done": True}
    ids = [r[0] for r in rows]
    wmill.set_flow_user_state("cursor", ids[-1])
    # One sub-cursor per parallel worker: starting position for each batch of `limit` rows
    sub_cursors = [cursor] + [ids[i - 1] for i in range(limit, len(ids), limit)]
    print(
        f"Fetched {len(ids)} ids, global cursor → {ids[-1]}, {len(sub_cursors)} workers"
    )
    return {"sub_cursors": sub_cursors, "done": False}
