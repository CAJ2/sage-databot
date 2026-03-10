# requirements: project

"""
Integration tests for f/search/* scripts.
Tests: index settings, category indexing, region indexing.
"""

import time

import meilisearch
import wmill
from sqlalchemy import text

from f.search.categories.index_categories import main as index_categories_main
from f.search.regions.index_regions import main as index_regions_main
from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_true
from f.utils.db.crdb import create_sql_engine
from f.utils.db.meili import check_create_index


def _wait_meili_idle(meili: meilisearch.Client, timeout: float = 30.0) -> None:
    """Wait until Meilisearch has no enqueued or processing tasks."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tasks = meili.get_tasks({"statuses": ["enqueued", "processing"]})
        if not tasks.results:
            return
        time.sleep(0.5)
    raise TimeoutError("Meilisearch did not finish processing tasks in time")


def _get_meili_client() -> meilisearch.Client:
    """Get a Meilisearch client for verification."""
    meili_res = wmill.get_resource("f/api_config/api_meilisearch")
    if meili_res is None:
        raise ValueError("No Meilisearch resource found")
    return meilisearch.Client(
        str(meili_res.get("api_url", "")),
        api_key=meili_res.get("api_key", None),
    )


def test_index_settings(t: Test):
    """Test that check_create_index configures index settings correctly."""
    meili = _get_meili_client()
    index_name = "test_settings_check"
    t.cleanup.track_meili_index(index_name)

    check_create_index(
        meili,
        index_name,
        {
            "searchableAttributes": ["name", "desc"],
            "filterableAttributes": ["category"],
        },
    )

    settings = meili.index(index_name).get_settings()
    assert_true("name" in settings["searchableAttributes"], "name should be searchable")


def test_index_categories(_t: Test):
    """Test category indexing via the real index_categories script."""
    crdb = create_sql_engine()
    with crdb.begin() as conn:
        rows = conn.execute(
            text("SELECT id FROM public.categories WHERE id != 'CATEGORY_ROOT' LIMIT 3")
        ).fetchall()

    if len(rows) == 0:
        print("  ⚠️  No categories in DB, skipping")
        return

    category_ids = [str(row[0]) for row in rows]
    index_categories_main(keys=category_ids)

    meili = _get_meili_client()
    _wait_meili_idle(meili)
    index = meili.index("categories_en")
    for cid in category_ids:
        doc = index.get_document(cid)
        assert_true(bool(doc), f"Category {cid} should be indexed")


def test_index_regions(_t: Test):
    """Test region indexing via the real index_regions script."""
    crdb = create_sql_engine()
    with crdb.begin() as conn:
        rows = conn.execute(text("SELECT id FROM public.regions LIMIT 3")).fetchall()

    if len(rows) == 0:
        print("  ⚠️  No regions in DB, skipping")
        return

    region_ids = [str(row[0]) for row in rows]
    index_regions_main(keys=region_ids)

    meili = _get_meili_client()
    _wait_meili_idle(meili)
    index = meili.index("regions_en")
    for rid in region_ids:
        doc = index.get_document(rid)
        assert_true(bool(doc), f"Region {rid} should be indexed")


def main() -> dict[str, object]:
    _ = ensure_test_workspace()
    suite = TestSuite("search")
    suite.run(test_index_settings)
    suite.run(test_index_categories)
    suite.run(test_index_regions)
    return suite.results()
