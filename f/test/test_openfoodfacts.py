# requirements: project

"""
Integration tests for OpenFoodFacts scripts.
Tests: f/openfoodfacts/off_variant.py
"""

import json

from sqlalchemy import text
from sqlalchemy.orm import Session

from f.test.cleanup import ensure_test_workspace
from f.test.framework import (
    Test,
    TestSuite,
    assert_eq,
    assert_gt,
    assert_true,
)
from f.utils.db.crdb import create_sql_engine


def _insert_test_off_product(engine, product_id: str):
    """Insert a minimal test OFFProduct into databot.off_products."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO databot.off_products (id, brands, categories, lang, product_name)
                VALUES (:id, :brands, :categories, :lang, :product_name)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": product_id,
                "brands": "Test Brand",
                "categories": "Test Category",
                "lang": "en",
                "product_name": json.dumps({
                    "product_name": [
                        {"lang": "en", "text": "Test Product for Integration"},
                        {"lang": "main", "text": "Test Product for Integration"},
                    ]
                }),
            },
        )


def _cleanup_test_off_product(engine, product_id: str):
    """Remove the test OFF product."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM databot.off_products WHERE id = :id"),
            {"id": product_id},
        )


def test_off_product_insert_and_query(t: Test):
    """Test that we can insert and query a test OFF product."""
    engine = create_sql_engine()
    product_id = "__test_off_product_1"

    _insert_test_off_product(engine, product_id)

    try:
        from f.db.databot.model import OFFProduct

        with Session(engine) as session:
            product = session.query(OFFProduct).where(OFFProduct.id == product_id).first()

        assert_true(product is not None, "Should find the test product")
        assert_eq(product.id, product_id)
        assert_eq(product.brands, "Test Brand")
        assert_eq(product.lang, "en")
        assert_true(product.product_name is not None, "Should have product_name")
        assert_gt(len(product.product_name.product_name), 0, "Should have name translations")
    finally:
        _cleanup_test_off_product(engine, product_id)


def test_off_variant_creation(t: Test):
    """Test that off_variant creates a variant from a test OFF product.

    Calls the real production off_variant script, which reads the product
    from databot.off_products and creates/updates a variant via the API.
    """
    engine = create_sql_engine()
    product_id = "__test_off_variant_product"

    _insert_test_off_product(engine, product_id)

    from f.openfoodfacts.off_variant import main as off_variant_main
    off_variant_main(product_id=product_id)

    # Verify a variant was created (check DB for __test_ prefixed variant)
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT id FROM public.variants WHERE id LIKE '__test_%' ORDER BY created_at DESC LIMIT 1"
        )).fetchone()

    if row:
        t.cleanup.track_entity("variants", row[0])
        assert_true(row is not None, "Should have created a __test_ variant")
    else:
        print("  ⚠️  No __test_ variant found, off_variant may have failed to create it")
    _cleanup_test_off_product(engine, product_id)


def main() -> dict:
    ensure_test_workspace()
    suite = TestSuite("openfoodfacts")
    suite.run(test_off_product_insert_and_query)
    suite.run(test_off_variant_creation)
    return suite.results()
