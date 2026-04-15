# requirements: project

import json
from typing import Any

import wmill
from sqlalchemy import text

from f.db.databot.model import ensure_cache_tables
from f.utils.db.crdb import create_sql_engine
from f.utils.log import BatchProgress, cfg_log


def main(batch_size: int = 1000):
    cfg_log()
    crdb = create_sql_engine()
    ensure_cache_tables(crdb)

    state = wmill.get_state() or {}
    current_id = state.get("last_cursor", "")
    print(f"Starting from cursor: {current_id}")

    with crdb.connect() as conn:
        total_count_stmt = (
            "SELECT COUNT(id) FROM databot.off_products WHERE images IS NOT NULL"
        )
        if current_id:
            total_count_stmt += " AND id > :cursor"

        total_count = (
            conn.execute(
                text(total_count_stmt),
                {"cursor": current_id},
            ).scalar()
            or 0
        )
        print(f"Total products to process: {total_count}")
        progress = BatchProgress(total_count)
        # Restore processed count if we are resuming
        progress.processed = state.get("processed", 0)

        total_processed_this_run = 0
        processed_variants: set[str] = set()

        while True:
            rows = conn.execute(
                text(
                    """
                    SELECT id, images
                    FROM databot.off_products
                    WHERE id > :cursor AND images IS NOT NULL
                    ORDER BY id
                    LIMIT :limit
                """
                ),
                {"cursor": current_id, "limit": batch_size},
            ).fetchall()

            if not rows:
                break

            for row in rows:
                product_id = row.id
                off_barcode = product_id.replace("off_", "")

                # Parse images to get key -> imgid mapping
                images_data = row.images
                if isinstance(images_data, str):
                    try:
                        images_data = json.loads(images_data)
                    except json.JSONDecodeError:
                        images_data = {}

                # Depending on how JSONData works, it might be a dict or a model
                # Based on model.py, it's likely a dict or an object with an 'images' list
                img_list = []
                if isinstance(images_data, dict):
                    img_list = images_data.get("images", [])
                elif hasattr(images_data, "images"):
                    img_list = images_data.images or []

                key_to_imgid: dict[str, int] = {}
                for img in img_list:
                    if isinstance(img, dict):
                        k = str(img.get("key", ""))
                        iid = img.get("imgid")
                    else:
                        # Handle Pydantic model
                        k = str(getattr(img, "key", ""))
                        iid = getattr(img, "imgid", None)

                    if k and iid is not None:
                        try:
                            key_to_imgid[k] = int(iid)
                        except (ValueError, TypeError):
                            pass

                # Get associated variant
                variant_rows = conn.execute(
                    text(
                        """
                        SELECT variant_id
                        FROM public.external_sources
                        WHERE source = 'OFF' AND source_id = :barcode
                    """
                    ),
                    {"barcode": off_barcode},
                ).fetchall()

                for v_row in variant_rows:
                    variant_id = v_row.variant_id
                    if not variant_id or variant_id in processed_variants:
                        continue
                    processed_variants.add(variant_id)

                    # Fetch all OFF image sources for this variant
                    sources = conn.execute(
                        text(
                            """
                            SELECT s.id, s.metadata, vs.meta
                            FROM public.sources s
                            JOIN public.variants_sources vs ON s.id = vs.source_id
                            WHERE vs.variant_id = :variant_id
                              AND s.metadata->>'parent_source' = 'g6OJVnSzQkE0mHtYS31O9'
                        """
                        ),
                        {"variant_id": variant_id},
                    ).fetchall()

                    if not sources:
                        continue

                    # Sort sources to determine unique orders
                    def get_sort_key(s_row: Any) -> tuple[int, Any, Any, str]:
                        meta = s_row.metadata
                        if isinstance(meta, str):
                            try:
                                meta = json.loads(meta)
                            except json.JSONDecodeError:
                                meta = {}
                        meta = meta or {}
                        key = str(meta.get("key", ""))

                        # Priority: front_* first (ordered by imgid from databot.off_products), then numeric keys, then others
                        if key.startswith("front_"):
                            imgid = key_to_imgid.get(key, 0)
                            return (0, imgid, key, s_row.id)
                        try:
                            val = int(key)
                            return (1, val, "", s_row.id)
                        except ValueError:
                            return (2, key, "", s_row.id)

                    sorted_sources = sorted(sources, key=get_sort_key)

                    for idx, s_row in enumerate(sorted_sources, start=1):
                        source_id = s_row.id
                        new_order = idx

                        s_meta = s_row.metadata
                        if isinstance(s_meta, str):
                            try:
                                s_meta = json.loads(s_meta)
                            except json.JSONDecodeError:
                                s_meta = {}
                        s_meta = s_meta or {}

                        vs_meta = s_row.meta
                        if isinstance(vs_meta, str):
                            try:
                                vs_meta = json.loads(vs_meta)
                            except json.JSONDecodeError:
                                vs_meta = {}
                        vs_meta = vs_meta or {}

                        s_needs_update = s_meta.get("order") != new_order
                        vs_needs_update = vs_meta.get("order") != new_order

                        if s_needs_update or vs_needs_update:
                            with conn.begin_nested():
                                if s_needs_update:
                                    new_s_meta = dict(s_meta)
                                    new_s_meta["order"] = new_order
                                    conn.execute(
                                        text(
                                            "UPDATE public.sources SET metadata = :meta WHERE id = :id"
                                        ),
                                        {
                                            "meta": json.dumps(new_s_meta),
                                            "id": source_id,
                                        },
                                    )
                                if vs_needs_update:
                                    new_vs_meta = dict(vs_meta)
                                    new_vs_meta["order"] = new_order
                                    conn.execute(
                                        text(
                                            "UPDATE public.variants_sources SET meta = :meta WHERE variant_id = :vid AND source_id = :sid"
                                        ),
                                        {
                                            "meta": json.dumps(new_vs_meta),
                                            "vid": variant_id,
                                            "sid": source_id,
                                        },
                                    )

                current_id = product_id
                progress.update(product_id)

            conn.commit()
            total_processed_this_run += len(rows)
            print(f"Processed {total_processed_this_run} products this run.")

    print("Finished updating image orders.")


if __name__ == "__main__":
    main()
