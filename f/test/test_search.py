# requirements: project

"""Integration tests for f/search/* scripts."""

from datetime import UTC, datetime

import polars as pl
from sqlalchemy import text

from f.search.categories.index_categories import main as index_categories_main
from f.search.items.index_items import prepend_category_names
from f.search.materials.index_materials import prepend_ancestor_names
from f.search.regions.index_regions import main as index_regions_main
from f.search.variants.index_variants import (
    barcode_forms,
    prepend_item_names,
    should_index_variant,
)
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


def test_prepend_category_names(_t: Test):
    desc = {"xx": "Simple fallback", "fr": "Description francaise"}
    categories = [
        {"id": "cat-1", "name": {"xx": "Fruit", "fr": "Fruit", "sv": "Frukt"}},
        {"id": "cat-2", "name": {"xx": "Snack"}},
        {"id": "cat-3", "name": {"en": "Fruit"}},
        {"id": "cat-4", "name": {"xx": "Shelf Stable"}},
        {"id": "cat-5", "name": {"xx": "Pantry"}},
        {"id": "cat-6", "name": {"xx": "Extra"}},
    ]

    merged = prepend_category_names(desc, categories)

    assert_true(
        merged["en"]
        == "Categories:\nFruit\nSnack\nShelf Stable\nPantry\nExtra\nSimple fallback",
        "English desc should use the Categories block and xx fallback, capped at five names",
    )
    assert_true(
        merged["fr"] == "Categories:\nFruit\nDescription francaise",
        "French desc should prepend translated category names with the Categories block",
    )
    assert_true(
        merged["sv"] == "Categories:\nFrukt\n",
        "Languages without desc should still get a newline-terminated Categories block",
    )
    assert_true(
        merged["xx"] == "Simple fallback",
        "The fallback desc should remain available for API use",
    )


def test_prepend_ancestor_names(_t: Test):
    desc = {"xx": "Base material", "fr": "Materiau de base"}
    ancestors = [
        {"id": "mat-1", "name": {"xx": "Plant Fiber", "fr": "Fibre vegetale"}},
        {"id": "mat-2", "name": {"xx": "Cellulose"}},
        {"id": "mat-3", "name": {"en": "Biomass"}},
    ]

    merged = prepend_ancestor_names(desc, ancestors)

    assert_true(
        merged["en"] == "Ancestors:\nPlant Fiber\nCellulose\nBiomass\nBase material",
        "English desc should prepend ancestor names using xx fallback",
    )
    assert_true(
        merged["fr"] == "Ancestors:\nFibre vegetale\nMateriau de base",
        "French desc should prepend translated ancestor names",
    )
    assert_true(
        merged["xx"] == "Base material",
        "The fallback desc should remain available for API use",
    )


def test_prepend_item_names(_t: Test):
    desc = {"xx": "Base variant", "fr": "Variante de base"}
    items = [
        {"id": "item-1", "name": {"xx": "Bottle", "fr": "Bouteille"}},
        {"id": "item-2", "name": {"xx": "Cap"}},
        {"id": "item-3", "name": {"en": "Label"}},
        {"id": "item-4", "name": {"xx": "Carton"}},
        {"id": "item-5", "name": {"xx": "Tray"}},
        {"id": "item-6", "name": {"xx": "Extra"}},
    ]

    merged = prepend_item_names(desc, items)

    assert_true(
        merged["en"] == "Items:\nBottle\nCap\nLabel\nCarton\nTray\nBase variant",
        "English desc should prepend up to five item names using xx fallback",
    )
    assert_true(
        merged["fr"] == "Items:\nBouteille\nVariante de base",
        "French desc should prepend translated item names",
    )
    assert_true(
        merged["xx"] == "Base variant",
        "The fallback desc should remain available for API use",
    )


def test_should_index_variant(_t: Test):
    assert_true(
        should_index_variant({"en": "Bottle 500ml"}),
        "Variants with a non-numeric English name should be indexed",
    )
    assert_true(
        should_index_variant({"xx": "Fallback Name"}),
        "Variants should fall back to xx when en is missing",
    )
    assert_true(
        not should_index_variant({"en": "12345"}),
        "Variants with an entirely numeric effective English name should be skipped",
    )
    assert_true(
        not should_index_variant({"xx": "007"}),
        "Variants with an entirely numeric xx fallback name should be skipped",
    )
    assert_true(
        not should_index_variant({"fr": "Nom seulement"}),
        "Variants without en and xx translations should be skipped",
    )
    assert_true(
        not should_index_variant({"en": "AB"}),
        "Variants with an effective English name shorter than 3 characters should be skipped",
    )
    assert_true(
        not should_index_variant({"xx": "Xy"}),
        "Variants with an xx fallback name shorter than 3 characters should be skipped",
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
    suite.run(test_prepend_category_names)
    suite.run(test_prepend_ancestor_names)
    suite.run(test_prepend_item_names)
    suite.run(test_should_index_variant)
    suite.run(test_index_categories)
    suite.run(test_index_regions)
    suite.run(test_barcode_forms)
    return suite.results()
