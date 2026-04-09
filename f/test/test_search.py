# requirements: project

"""Integration tests for f/search/* scripts."""

from datetime import UTC, datetime

import polars as pl
from sqlalchemy import text

from f.search.categories.index_categories import main as index_categories_main
from f.search.regions.index_regions import main as index_regions_main
from f.search.variants.index_variants import barcode_forms
from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_true
from f.utils.db.crdb import create_sql_engine
from f.utils.db.typesense import (
    check_create_collection,
    ts_connect,
    translated_schema_fields,
    with_unix_timestamps,
)
from typing import cast


def _field_map(schema: object) -> dict[str, dict[str, object]]:
    schema_dict = cast(dict[str, object], schema) if isinstance(schema, dict) else {}
    fields = cast(list[object], schema_dict.get("fields", []))
    return {
        field["name"]: field
        for field in fields
        if isinstance(field, dict) and "name" in field
    }


def test_collection_schema(t: Test):
    """Test that check_create_collection creates the expected Typesense schema."""
    ts = ts_connect()
    collection_name = "test_settings_check"
    t.cleanup.track_typesense_collection(collection_name)

    check_create_collection(
        ts,
        collection_name,
        [
            {"name": "id", "type": "string"},
            {"name": "updated_at", "type": "int64", "sort": True},
            {"name": "category", "type": "string", "facet": True},
            *translated_schema_fields({"name": "string", "desc": "string"}),
        ],
    )

    schema = ts.collections[collection_name].retrieve()
    fields = _field_map(schema)
    assert_true("name_en" in fields, "name_en field should exist")
    assert_true(fields["name_en"].get("locale") == "en", "name_en should use en locale")
    assert_true(bool(fields["name_en"].get("stem")), "name_en should enable stemming")
    assert_true(bool(fields["category"].get("facet")), "category should be facetable")
    assert_true(
        fields["updated_at"].get("type") == "int64", "updated_at should be int64"
    )
    assert_true(bool(fields["updated_at"].get("sort")), "updated_at should be sortable")


def test_with_unix_timestamps(_t: Test):
    df = with_unix_timestamps(
        pl.DataFrame({"updated_at": [datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)]}),
        ["updated_at"],
    )
    updated_at = df.to_dicts()[0]["updated_at"]
    assert_true(
        updated_at == 1704164645, "updated_at should be converted to unix seconds"
    )


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

    ts = ts_connect()
    for cid in category_ids:
        doc = ts.collections["categories"].documents[cid].retrieve()
        assert_true(bool(doc), f"Category {cid} should be indexed")
        assert_true("name_en" in doc, f"Category {cid} should have name_en")
        assert_true(
            isinstance(doc.get("updated_at"), int),
            f"Category {cid} should store updated_at as unix seconds",
        )


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

    ts = ts_connect()
    for rid in region_ids:
        doc = ts.collections["regions"].documents[rid].retrieve()
        assert_true(bool(doc), f"Region {rid} should be indexed")
        assert_true("name_en" in doc, f"Region {rid} should have name_en")
        assert_true(
            isinstance(doc.get("updated_at"), int),
            f"Region {rid} should store updated_at as unix seconds",
        )


def test_barcode_forms(_t: Test):
    assert_true(
        barcode_forms("0123456789012")
        == ["0123456789012", "123456789012", "00123456789012"],
        "EAN-13 / UPC-A forms should be indexed together",
    )
    assert_true(
        barcode_forms("0000012345678")
        == [
            "0000012345678",
            "12345678",
            "000012345678",
            "00000012345678",
        ],
        "EAN-8 and zero-padded GTIN forms should be indexed together",
    )
    assert_true(
        barcode_forms("1234567")
        == ["1234567", "01234567", "000001234567", "0000001234567", "00000001234567"],
        "Short UPC/EAN forms should be expanded with leading-zero variants",
    )


def main() -> dict[str, object]:
    _ = ensure_test_workspace()
    suite = TestSuite("search")
    suite.run(test_collection_schema)
    suite.run(test_with_unix_timestamps)
    suite.run(test_index_categories)
    suite.run(test_index_regions)
    suite.run(test_barcode_forms)
    return suite.results()
