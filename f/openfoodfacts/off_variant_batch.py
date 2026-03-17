# requirements: project

from sqlalchemy import text

from f.openfoodfacts.off_variant import off_variant
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine
from f.utils.db.meili import meili_client
from f.utils.log import cfg_log


def main(start_cursor: str, end_cursor: str = "", batch_size: int = 100):
    cfg_log()
    crdb = create_sql_engine()
    meili = meili_client()
    client, _ = api_connect()

    current = start_cursor
    first = True
    total = 0

    while True:
        op = ">=" if first else ">"
        first = False

        params: dict[str, object] = {"cursor": current, "limit": batch_size}
        cond = f"id {op} :cursor"
        if end_cursor:
            cond += " AND id <= :end"
            params["end"] = end_cursor

        with crdb.begin() as conn:
            rows = conn.execute(
                text(
                    f"SELECT id FROM databot.off_products WHERE {cond} ORDER BY id LIMIT :limit"
                ),
                params,
            ).fetchall()

        if not rows:
            break

        product_ids = [row[0] for row in rows]

        for pid in product_ids:
            try:
                off_variant(pid, crdb=crdb, meili=meili, client=client)
            except Exception as e:
                print(f"Error processing {pid}: {e}")

        total += len(product_ids)
        print(f"Processed {total} products so far (last: {product_ids[-1]})")
        current = product_ids[-1]

        if len(rows) < batch_size:
            break

    print(f"Done. Total processed: {total}")
