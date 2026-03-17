# requirements: project

import wmill
from sqlalchemy import text

from f.db.databot.model import ensure_cache_tables
from f.openfoodfacts.off_download_images import off_download_images
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.log import cfg_log
from f.utils.s3 import S3Client


def main(start_cursor: str = "", end_cursor: str = "", batch_size: int = 100):
    cfg_log()
    crdb = create_sql_engine()
    ensure_cache_tables(crdb)
    client, _ = api_connect()
    kg_api_key = wmill.get_variable("f/api_config/gcp_kg_api_key")
    s3_client = S3Client(resource_id="f/s3_config/s3_sources")

    current = start_cursor
    first = True
    total = 0

    while True:
        op = ">=" if first else ">"
        first = False

        params: dict[str, object] = {"limit": batch_size}
        if current:
            cond = f"source = 'OFF' AND source_id {op} :cursor"
            params["cursor"] = current
        else:
            cond = "source = 'OFF'"
        if end_cursor:
            cond += " AND source_id <= :end"
            params["end"] = end_cursor

        with crdb.begin() as conn:
            rows = conn.execute(
                text(
                    f"SELECT source_id, variant_id FROM public.external_sources WHERE {cond} ORDER BY source_id LIMIT :limit"
                ),
                params,
            ).fetchall()

        if not rows:
            break

        for source_id, variant_id in rows:
            if not variant_id:
                print(f"Skipping source_id {source_id}: no variant_id")
                continue
            try:
                off_download_images(
                    variant_id,
                    crdb=crdb,
                    client=client,
                    kg_api_key=kg_api_key,
                    s3_client=s3_client,
                )
            except Exception as e:
                print(
                    f"Error processing variant {variant_id} (source_id {source_id}): {e}"
                )

        total += len(rows)
        print(f"Processed {total} rows so far (last source_id: {rows[-1][0]})")
        current = rows[-1][0]

        if len(rows) < batch_size:
            break

    print(f"Done. Total processed: {total}")
