# requirements: project
import wmill
from sqlalchemy import text

from f.db.databot.model import ensure_batch_job_tables
from f.utils.db.crdb import create_sql_engine


def main(batch_size: int, start_cursor: str = ""):
    engine = create_sql_engine()
    ensure_batch_job_tables(engine)
    cursor = wmill.get_flow_user_state("process_sources_cursor") or start_cursor
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT id FROM public.sources
                WHERE type = 'IMAGE'
                  AND location IS NOT NULL
                  AND processed_at IS NULL
                  AND id > :cursor
                  AND id NOT IN (
                      SELECT bji.source_id FROM databot.batch_job_items bji
                      JOIN databot.batch_jobs bj ON bj.id = bji.job_id
                      WHERE bj.status NOT IN ('SUCCESS','FAILED','TIMEOUT_EXCEEDED','CANCELLED')
                  )
                ORDER BY id LIMIT :limit
            """),
            {"cursor": cursor, "limit": batch_size},
        ).fetchall()
    ids = [r[0] for r in rows]
    if ids:
        wmill.set_flow_user_state("process_sources_cursor", ids[-1])
    print(f"Fetched {len(ids)} source id(s), cursor={ids[-1] if ids else 'done'}")
    return {"source_ids": ids, "count": len(ids)}
