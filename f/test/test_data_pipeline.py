# requirements: project

"""
Integration tests for data pipeline scripts.
Tests: f/categories/categories_flow.py, f/tags/tags_flow.py
"""

from sqlalchemy import text

import f.tags.component_tags as component_tags
import f.tags.place_tags as place_tags
import f.tags.variant_tags as variant_tags
from f.categories.categories_flow import main as categories_flow_main
from f.tags.tags_flow import main as tags_flow_main
from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_gt, assert_true
from f.utils.db.crdb import create_sql_engine


def test_tags_definitions_valid(_t: Test):
    """Test that tag definitions are valid and have required fields."""

    all_tags = []
    for tags in [variant_tags.tags, component_tags.tags, place_tags.tags]:
        all_tags.extend(tags)

    assert_gt(len(all_tags), 0, "Should have at least one tag definition")

    required_keys = {"id", "name", "type", "tag_id"}
    for tag in all_tags:
        for key in required_keys:
            assert_true(
                key in tag, f"Tag {tag.get('id', '?')} missing required key '{key}'"
            )
        assert_true(
            isinstance(tag["name"], dict), f"Tag {tag['id']} name should be a dict"
        )
        assert_true(
            tag["type"] in ("VARIANT", "COMPONENT", "PLACE"),
            f"Tag {tag['id']} has invalid type '{tag['type']}'",
        )


def test_categories_flow(_t: Test):
    """Test that the categories flow reads TSVs, validates DAG, and upserts to CRDB."""

    categories_flow_main()

    engine = create_sql_engine()
    with engine.begin() as conn:
        cat_count = conn.execute(
            text("SELECT COUNT(*) FROM public.categories WHERE id != 'CATEGORY_ROOT'")
        ).scalar()
        assert_gt(cat_count, 0, "Should have categories in DB after flow")

        edge_count = conn.execute(
            text("SELECT COUNT(*) FROM public.category_edges")
        ).scalar()
        assert_gt(edge_count, 0, "Should have category_edges in DB after flow")


def test_tags_flow(_t: Test):
    """Test that the tags flow reads definitions and upserts to CRDB."""

    tags_flow_main()

    engine = create_sql_engine()
    with engine.begin() as conn:
        tag_count = conn.execute(text("SELECT COUNT(*) FROM public.tags")).scalar()
        assert_gt(tag_count, 0, "Should have tags in DB after flow")


def main() -> dict[str, object]:
    ensure_test_workspace()
    suite = TestSuite("data_pipeline")
    suite.run(test_tags_definitions_valid)
    suite.run(test_categories_flow)
    suite.run(test_tags_flow)
    return suite.results()
