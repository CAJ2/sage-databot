# requirements: project

"""
Integration tests for f/changes/* and f/context/* scripts.
Tests: analyze_*_edit, all context scripts.
"""

from sqlalchemy import text

from f.changes.analyze_category_edit import main as analyze_category
from f.changes.analyze_variant_edit import main as analyze_variant
from f.context.category_context import main as category_context_main
from f.context.component_context import main as component_context_main
from f.context.item_context import main as item_context_main
from f.context.variant_context import main as variant_context_main
from f.test.cleanup import ensure_test_workspace
from f.test.framework import (
    Test,
    TestSuite,
    assert_contains,
    assert_eq,
    assert_isinstance,
    assert_true,
)
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine


def _find_existing_entity(client, entity_type: str) -> str | None:
    """Find an existing entity ID to use for read-only tests."""
    try:
        if entity_type == "variant":
            engine = create_sql_engine()
            with engine.begin() as conn:
                row = conn.execute(text(
                    "SELECT id FROM public.variants LIMIT 1"
                )).fetchone()
                return row[0] if row else None
        elif entity_type == "category":
            result = client.get_root_category()
            if result and result.category:
                return result.category.id
        elif entity_type == "item":
            engine = create_sql_engine()
            with engine.begin() as conn:
                row = conn.execute(text(
                    "SELECT id FROM public.items LIMIT 1"
                )).fetchone()
                return row[0] if row else None
    except Exception as e:
        print(f"  Could not find existing {entity_type}: {e}")
    return None


# --- Context tests ---

def test_variant_context(t: Test):
    """Test that variant_context returns valid EntityContext."""
    client, _ = api_connect()
    variant_id = _find_existing_entity(client, "variant")
    if not variant_id:
        print("  ⚠️  No variant found, skipping")
        return

    result = variant_context_main(entity_id=variant_id, mode="review")
    result = result.model_dump()

    assert_isinstance(result, dict, "Should return a dict (EntityContext)")
    assert_eq(result["entity_name"], "Variant")
    assert_eq(result["entity_id"], variant_id)
    assert_true("entity_data" in result, "Should have entity_data")
    assert_true("prompt_hints" in result, "Should have prompt_hints")


def test_variant_context_suggest_mode(t: Test):
    """Test variant_context in suggest mode."""
    client, _ = api_connect()
    variant_id = _find_existing_entity(client, "variant")
    if not variant_id:
        print("  ⚠️  No variant found, skipping")
        return

    result = variant_context_main(
        entity_id=variant_id,
        mode="suggest",
        target_fields=["name", "desc"],
    )
    result = result.model_dump()

    assert_isinstance(result, dict)
    assert_contains(result["prompt_hints"], "suggest",
                    "Suggest mode should mention 'suggest' in hints")


def test_category_context(t: Test):
    """Test that category_context returns valid EntityContext."""
    client, _ = api_connect()
    cat_id = _find_existing_entity(client, "category")
    if not cat_id:
        print("  ⚠️  No category found, skipping")
        return

    result = category_context_main(entity_id=cat_id, mode="review")
    result = result.model_dump()

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Category")
    assert_eq(result["entity_id"], cat_id)


def test_item_context(t: Test):
    """Test that item_context returns valid EntityContext."""
    client, _ = api_connect()
    item_id = _find_existing_entity(client, "item")
    if not item_id:
        print("  ⚠️  No item found, skipping")
        return

    result = item_context_main(entity_id=item_id, mode="review")
    result = result.model_dump()

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Item")


def test_generic_context(t: Test):
    """Test generic_context with a component."""
    engine = create_sql_engine()
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT id FROM public.components LIMIT 1"
        )).fetchone()

    if not row:
        print("  ⚠️  No component found, skipping")
        return

    result = component_context_main(entity_id=row[0], mode="review")
    result = result.model_dump()

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Component")


# --- Changes tests ---

def test_analyze_variant_edit_structure(t: Test):
    """Test that analyze_variant_edit returns a valid EditAnalysis structure."""
    client, _ = api_connect()
    variant_id = _find_existing_entity(client, "variant")
    if not variant_id:
        print("  ⚠️  No variant found, skipping")
        return

    try:
        result = analyze_variant(
            change_id="__test_change_1",
            edit_id="__test_edit_1",
            entity_name="Variant",
            create_changes=None,
            update_changes={"nameTr": [{"lang": "en", "text": "Test Name Updated"}]},
            proposed_id=None,
            original_id=variant_id,
        )
    except Exception as e:
        if "llm" in str(e).lower() or "model" in str(e).lower() or "api" in str(e).lower():
            print(f"  ⚠️  Skipping LLM-dependent test: {e}")
            return
        raise

    result = result.model_dump()
    assert_isinstance(result, dict, "Should return a dict (EditAnalysis)")
    assert_true("edit_id" in result, "Should have edit_id field")


def test_analyze_category_edit_structure(t: Test):
    """Test analyze_category_edit returns a valid structure."""
    client, _ = api_connect()
    cat_id = _find_existing_entity(client, "category")
    if not cat_id:
        print("  ⚠️  No category found, skipping")
        return

    result = analyze_category(
        change_id="__test_change_2",
        edit_id="__test_edit_2",
        entity_name="Category",
        create_changes=None,
        update_changes={"nameTr": [{"lang": "en", "text": "Test Category"}]},
        proposed_id=None,
        original_id=cat_id,
    )

    assert_true(result is not None, "Should return a result")


def main() -> dict[str, object]:
    ensure_test_workspace()
    suite = TestSuite("changes_and_context")
    suite.run(test_variant_context)
    suite.run(test_variant_context_suggest_mode)
    suite.run(test_category_context)
    suite.run(test_item_context)
    suite.run(test_generic_context)
    suite.run(test_analyze_variant_edit_structure)
    suite.run(test_analyze_category_edit_structure)
    return suite.results()
