# requirements: project

"""Integration tests for f/search/* scripts."""

import base64
from datetime import UTC, datetime
from io import BytesIO
from typing import cast

import polars as pl
from PIL import Image
from sqlalchemy import text

from f.search.categories.index_categories import main as index_categories_main
from f.search.items.index_items import prepend_category_names
from f.search.materials.index_materials import prepend_ancestor_names
from f.search.regions.index_regions import main as index_regions_main
from f.search.variants.index_variants import (
    append_image_context,
    barcode_forms,
    clip_image_base64,
    prepend_component_names,
    prepend_item_names,
    should_index_variant,
)
from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_true
from f.utils.db.crdb import create_sql_engine
from f.utils.db.typesense import (
    add_mistral_embeddings,
    check_create_collection,
    mistral_embedding_field,
    mistral_embedding_input,
    translated_field_names,
    translated_schema_fields,
    ts_connect,
    with_unix_timestamps,
)
from f.utils.urls import normalize_source_url


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


def test_mistral_embedding_field(_t: Test):
    field = mistral_embedding_field(["name_en", "desc_en", "desc_short_en"])

    assert_true(
        field["name"] == "embedding", "Embedding field should use the default name"
    )
    assert_true(field["type"] == "float[]", "Embedding field should use float vectors")
    assert_true(
        field["num_dim"] == 1024, "Embedding field should use mistral-embed dimensions"
    )
    assert_true(bool(field["optional"]), "Embedding field should be optional")
    assert_true(
        "embed" not in field,
        "Embedding field should be a plain vector field for manual indexing",
    )


def test_mistral_embedding_input(_t: Test):
    input_text = mistral_embedding_input(
        {
            "name_en": "Food",
            "desc_en": "Products that are edible",
            "desc_short_en": "Edible products",
        },
        ["name_en", "desc_en", "desc_short_en"],
    )
    assert_true(
        input_text == "Food\n\nProducts that are edible\n\nEdible products",
        "Embedding input should concatenate non-empty fields in order",
    )
    truncated_text = mistral_embedding_input(
        {"name_en": "x" * 500, "desc_en": "ignored"},
        ["name_en", "desc_en"],
    )
    assert_true(
        truncated_text == ("x" * 400),
        "Embedding input should be truncated to 400 characters",
    )


def test_add_mistral_embeddings(_t: Test):
    docs = [
        {"id": "cat-1", "name_en": "Food", "desc_en": "Products that are edible"},
        {"id": "cat-2", "name_en": "", "desc_en": ""},
    ]

    def fake_embedder(inputs: list[str]) -> list[list[float]]:
        assert_true(
            inputs == ["Food\n\nProducts that are edible"],
            "Manual embedder should only receive docs with embedding content",
        )
        return [[0.25] * 1024]

    embedded_docs = add_mistral_embeddings(
        docs,
        ["name_en", "desc_en"],
        embedder=fake_embedder,
    )

    assert_true(
        len(cast(list[object], embedded_docs[0]["embedding"])) == 1024,
        "Embedding vector should be attached to matching docs",
    )
    assert_true(
        "embedding" not in embedded_docs[1],
        "Docs without embedding input should not receive an embedding",
    )


def test_translated_field_names(_t: Test):
    assert_true(
        translated_field_names(["name", "desc"]) == ["name_en", "desc_en"],
        "Translated field names should default to English embedding fields only",
    )


def test_clip_image_base64(_t: Test):
    image = Image.new("RGB", (400, 300), color=(12, 34, 56))
    buf = BytesIO()
    image.save(buf, format="PNG")

    encoded = clip_image_base64(buf.getvalue())
    decoded = Image.open(BytesIO(base64.b64decode(encoded)))

    assert_true(decoded.size == (224, 224), "CLIP images should be cropped to 224x224")
    assert_true(decoded.mode == "RGB", "CLIP images should be converted to RGB")


def test_normalize_source_url(_t: Test):
    assert_true(
        normalize_source_url("cdn://sources/off/0096619937295/2.400.jpg")
        == "https://sources.sageleaf.app/off/0096619937295/2.400.jpg",
        "cdn://sources URLs should be mapped to the public sources host",
    )
    assert_true(
        normalize_source_url("https://example.com/image.jpg")
        == "https://example.com/image.jpg",
        "Non-cdn URLs should be left unchanged",
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


def test_prepend_component_names(_t: Test):
    desc = {"xx": "Base variant", "fr": "Variante de base"}
    components = [
        {"id": "component-1", "name": {"xx": "Bottle", "fr": "Bouteille"}},
        {"id": "component-2", "name": {"xx": "Cap"}},
        {"id": "component-3", "name": {"en": "Label"}},
        {"id": "component-4", "name": {"xx": "Carton"}},
        {"id": "component-5", "name": {"xx": "Tray"}},
        {"id": "component-6", "name": {"xx": "Extra"}},
    ]

    merged = prepend_component_names(desc, components)

    assert_true(
        merged["en"] == "Components:\nBottle\nCap\nLabel\nCarton\nTray\nBase variant",
        "English desc should prepend up to five component names using xx fallback",
    )
    assert_true(
        merged["fr"] == "Components:\nBouteille\nVariante de base",
        "French desc should prepend translated component names",
    )
    assert_true(
        merged["xx"] == "Base variant",
        "The fallback desc should remain available for API use",
    )


def test_append_image_context(_t: Test):
    desc = {"xx": "Base variant", "fr": "Variante de base"}

    merged = append_image_context(desc, "KIRKLAND\nORGANIC\nTOMATO PASTE")

    assert_true(
        merged["en"] == "Base variant\nImage context:\nKIRKLAND\nORGANIC\nTOMATO PASTE",
        "English desc should include OCR context with xx fallback",
    )
    assert_true(
        merged["xx"] == "Base variant",
        "The fallback desc should remain available for API use",
    )
    assert_true(
        merged["fr"] == "Variante de base",
        "Non-English desc values should be left untouched",
    )
    truncated = append_image_context({"en": "Base"}, "x" * 200)
    assert_true(
        truncated["en"] == f"Base\nImage:\n{'x' * 150}",
        "Image context should be truncated to 150 characters",
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
    suite.run(test_mistral_embedding_field)
    suite.run(test_mistral_embedding_input)
    suite.run(test_add_mistral_embeddings)
    suite.run(test_translated_field_names)
    suite.run(test_clip_image_base64)
    suite.run(test_normalize_source_url)
    suite.run(test_prepend_category_names)
    suite.run(test_prepend_ancestor_names)
    suite.run(test_prepend_item_names)
    suite.run(test_prepend_component_names)
    suite.run(test_append_image_context)
    suite.run(test_should_index_variant)
    suite.run(test_index_categories)
    suite.run(test_index_regions)
    suite.run(test_barcode_forms)
    return suite.results()
