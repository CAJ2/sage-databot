# requirements: project
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(
    sql_query: str,
    db_resource: str = "f/db_config/db_sage",
    max_items: int | None = None,
):
    engine = create_sql_engine(resource=db_resource)
    q = (
        f"SELECT * FROM ({sql_query}) AS _q LIMIT {max_items}"
        if max_items
        else sql_query
    )
    with engine.connect() as conn:
        result = conn.execute(text(q))
        rows = [dict(row._mapping) for row in result]
    print(f"Query returned {len(rows)} rows")
    return rows
