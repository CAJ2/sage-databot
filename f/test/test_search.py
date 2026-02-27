# requirements: project

"""
Integration tests for f/search/* scripts.
Tests: index settings, category indexing, region indexing.
"""

import meilisearch
import wmill
from sqlalchemy import text

from f.test.framework import Test, TestSuite, assert_true, assert_gt
from f.test.cleanup import ensure_test_workspace
from f.utils.db.crdb import create_sql_engine


def _get_meili_client() -> meilisearch.Client:
    """Get a Meilisearch client for verification."""
    meili_res = wmill.get_resource("f/api_config/api_meilisearch")
    if meili_res is None:
        raise ValueError("No Meilisearch resource found")
    return meilisearch.Client(
        meili_res.get("api_url"),
        api_key=meili_res.get("api_key", None),
    )


def test_index_settings(t: Test):
    """Test that check_create_index configures index settings correctly."""
    from f.search.index_script import check_create_index

    meili = _get_meili_client()
    index_name = "test_settings_check"
    t.cleanup.track_meili_index(index_name)

    check_create_index(meili, index_name, {
        "searchableAttributes": ["name", "desc"],
        "filterableAttributes": ["category"],
    })

    settings = meili.index(index_name).get_settings()
    assert_true("name" in settings["searchableAttributes"],
                "name should be searchable")


def test_index_categories(t: Test):
    """Test category indexing via the real index_categories script."""
    from f.search.categories.index_categories import main as index_categories_main

    crdb = create_sql_engine()
    with crdb.begin() as conn:
        rows = conn.execute(text(
            "SELECT id FROM public.categories WHERE id != 'CATEGORY_ROOT' LIMIT 3"
        )).fetchall()

    if len(rows) == 0:
        print("  ⚠️  No categories in DB, skipping")
        return

    category_ids = [row[0] for row in rows]
    index_categories_main(keys=category_ids)

    meili = _get_meili_client()
    index = meili.index("categories")
    for cid in category_ids:
        doc = index.get_document(cid)
        assert_true(doc is not None, f"Category {cid} should be indexed")


def test_index_regions(t: Test):
    """Test region indexing via the real index_regions script."""
    from f.search.regions.index_regions import main as index_regions_main

    crdb = create_sql_engine()
    with crdb.begin() as conn:
        rows = conn.execute(text(
            "SELECT id FROM public.regions LIMIT 3"
        )).fetchall()

    if len(rows) == 0:
        print("  ⚠️  No regions in DB, skipping")
        return

    region_ids = [row[0] for row in rows]
    index_regions_main(keys=region_ids)

    meili = _get_meili_client()
    index = meili.index("regions")
    for rid in region_ids:
        doc = index.get_document(rid)
        assert_true(doc is not None, f"Region {rid} should be indexed")


def main() -> dict:
    ensure_test_workspace()
    suite = TestSuite("search")
    suite.run(test_index_settings)
    suite.run(test_index_categories)
    suite.run(test_index_regions)
    return suite.results()
