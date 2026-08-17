# requirements: project
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(source_ids: list[str]):
    if not source_ids:
        return {"updated": 0}
    engine = create_sql_engine()
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE public.sources SET processed_at = now() WHERE id = ANY(:ids)"),
            {"ids": source_ids},
        )
    return {"updated": len(source_ids)}
