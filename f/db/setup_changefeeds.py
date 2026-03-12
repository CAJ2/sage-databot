# requirements: project
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine

TABLES = [
    "categories",
    "regions",
    "orgs",
    "variants",
    "components",
    "materials",
    "places",
    "items",
]
WEBHOOK_BASE_URL = (
    "webhook-https://databot.dev.sageleaf.app/api/w/sage-dev/jobs/run/f/f/search/index"
)
TOKEN_VARIABLE = "f/db_config/db_changefeed_webhook_token"
CHANGEFEED_OPTIONS = """WITH updated, initial_scan = 'no', webhook_sink_config = '{ "Flush": {"Messages": 100, "Frequency": "60s"}, "Retry": { "Max": 4, "Backoff": "10s"} }'"""


def _parse_table_names(full_table_names: list[str] | str | None) -> list[str]:
    """Parse full_table_names field which may be a list or a string like '{db.public.tbl}'."""
    if full_table_names is None:
        return []
    if isinstance(full_table_names, list):
        return [n.split(".")[-1] for n in full_table_names]
    # String format: {mydb.public.categories,mydb.public.items}
    cleaned = full_table_names.strip("{}")
    if not cleaned:
        return []
    return [n.split(".")[-1] for n in cleaned.split(",")]


def main():
    token = wmill.get_variable(TOKEN_VARIABLE)
    webhook_url = f"{WEBHOOK_BASE_URL}?token={token}&insecure_tls_skip_verify=true"

    # AUTOCOMMIT required — CREATE CHANGEFEED cannot run inside a transaction
    engine = create_sql_engine()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        # 1. Find tables with running/paused changefeeds
        rows = conn.execute(text("SHOW CHANGEFEED JOBS")).mappings().all()
        active_tables: set[str] = set()
        for row in rows:
            if row["status"] in ("running", "paused"):
                active_tables.update(_parse_table_names(row["full_table_names"]))

        # 2. Create missing changefeeds (one per table, one-at-a-time for AUTOCOMMIT)
        created = []
        for table in TABLES:
            if table not in active_tables:
                stmt = f"CREATE CHANGEFEED FOR TABLE public.{table} INTO '{webhook_url}' {CHANGEFEED_OPTIONS}"
                conn.execute(text(stmt))
                created.append(table)
                print(f"Created changefeed for table: {table}")

        # 3. Re-query and return status for all our tables
        rows = conn.execute(text("SHOW CHANGEFEED JOBS")).mappings().all()

    # Filter to rows that touch at least one of our tables
    our_jobs = []
    for row in rows:
        row_tables = _parse_table_names(row["full_table_names"])
        if any(t in TABLES for t in row_tables):
            our_jobs.append(
                {
                    "job_id": str(row["job_id"]),
                    "status": row["status"],
                    "tables": row_tables,
                    "error": row.get("error") or "",
                    "high_water_timestamp": str(row.get("high_water_timestamp") or ""),
                }
            )

    return {"created": created, "jobs": our_jobs}
