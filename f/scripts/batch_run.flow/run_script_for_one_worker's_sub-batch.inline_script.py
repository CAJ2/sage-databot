# requirements: project
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import wmill
from f.utils.db.crdb import create_sql_engine
from sqlalchemy import text


def main(
    sql_query: str,
    sub_cursor: str,
    limit: int,
    script_path: str,
    item_index: int = 0,
    delay_between_batches_s: float = 0,
    db_resource: str = "f/db_config/db_sage",
    static_args: dict[str, object] | None = None,
    cursor_column: str = "id",
):
    if delay_between_batches_s > 0 and item_index > 0:
        time.sleep(delay_between_batches_s)
    engine = create_sql_engine(resource=db_resource)
    with engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    f"SELECT * FROM ({sql_query}) AS q WHERE {cursor_column} > :cursor ORDER BY {cursor_column} LIMIT :limit"
                ),
                {"cursor": sub_cursor, "limit": limit},
            )
            .mappings()
            .fetchall()
        )
    rows = [dict(r) for r in rows]
    print(f"Worker {item_index}: processing {len(rows)} rows (cursor={sub_cursor})")

    def run_row(row: dict[str, object]) -> None:
        args = {**(static_args or {}), **row}
        wmill.run_script_by_path(
            script_path, args=args, assert_result_is_not_none=False
        )

    with ThreadPoolExecutor(max_workers=len(rows) or 1) as pool:
        futures = [pool.submit(run_row, row) for row in rows]
        for f in as_completed(futures):
            f.result()
    return {"processed": len(rows)}
