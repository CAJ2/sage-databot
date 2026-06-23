# requirements: project

"""
Integration tests for f/changes/* and f/context/* scripts.
Tests: all context scripts.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from sqlalchemy import text

from f.changes.ai_associate import existing_association_ids, fallback_target_ids
from f.changes.apply_associations import build_apply_requests
from f.context.context_types import EntityContext
from f.context.context_helpers import fetch_context_entity, fetch_context_schema
from f.context.category_context import main as category_context_main
from f.context.component_context import main as component_context_main
from f.context.item_context import main as item_context_main
from f.context.variant_context import main as variant_context_main
from f.graphql.api_client.enums import SearchType
from f.test.cleanup import ensure_test_workspace
from f.test.framework import (
    Test,
    TestSuite,
    assert_contains,
    assert_eq,
    assert_isinstance,
    assert_raises,
    assert_true,
)
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine


def _find_existing_entity(client: Any, entity_type: str) -> str | None:
    """Find an existing entity ID to use for read-only tests."""
    try:
        if entity_type == "variant":
            engine = create_sql_engine()
            with engine.begin() as conn:
                row = conn.execute(
                    text("SELECT id FROM public.variants LIMIT 1")
                ).fetchone()
                return row[0] if row else None
        elif entity_type == "category":
            result = client.get_root_category()
            if result and result.category:
                return result.category.id
        elif entity_type == "item":
            engine = create_sql_engine()
            with engine.begin() as conn:
                row = conn.execute(
                    text("SELECT id FROM public.items LIMIT 1")
                ).fetchone()
                return row[0] if row else None
    except Exception as e:
        print(f"  Could not find existing {entity_type}: {e}")
    return None


# --- Context tests ---


def test_variant_context(_t: Test):
    """Test that variant_context returns valid EntityContext."""
    client, _ = api_connect()
    variant_id = _find_existing_entity(client, "variant")
    if not variant_id:
        print("  ⚠️  No variant found, skipping")
        return

    result = variant_context_main(entity_id=variant_id, mode="review")

    assert_isinstance(result, dict, "Should return a dict (EntityContext)")
    assert_eq(result["entity_name"], "Variant")
    assert_eq(result["entity_id"], variant_id)
    assert_true("entity_data" in result, "Should have entity_data")
    assert_true("prompt_hints" in result, "Should have prompt_hints")


def test_variant_context_suggest_mode(_t: Test):
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

    assert_isinstance(result, dict)
    assert_contains(
        result["prompt_hints"],
        "suggest",
        "Suggest mode should mention 'suggest' in hints",
    )


def test_category_context(_t: Test):
    """Test that category_context returns valid EntityContext."""
    client, _ = api_connect()
    cat_id = _find_existing_entity(client, "category")
    if not cat_id:
        print("  ⚠️  No category found, skipping")
        return

    result = category_context_main(entity_id=cat_id, mode="review")

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Category")
    assert_eq(result["entity_id"], cat_id)


def test_item_context(_t: Test):
    """Test that item_context returns valid EntityContext."""
    client, _ = api_connect()
    item_id = _find_existing_entity(client, "item")
    if not item_id:
        print("  ⚠️  No item found, skipping")
        return

    result = item_context_main(entity_id=item_id, mode="review")

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Item")


def test_generic_context(_t: Test):
    """Test generic_context with a component."""
    engine = create_sql_engine()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM public.components LIMIT 1")).fetchone()

    if not row:
        print("  ⚠️  No component found, skipping")
        return

    result = component_context_main(entity_id=row[0], mode="review")

    assert_isinstance(result, dict)
    assert_eq(result["entity_name"], "Component")


def test_existing_association_ids(_t: Test):
    result = existing_association_ids(
        {
            "variants": [
                {"id": "var_1", "name": "Variant 1"},
                {"id": "var_1", "name": "Duplicate"},
                "var_2",
                {"name": "Missing ID"},
            ]
        },
        "variants",
    )

    assert_eq(result, ["var_1", "var_2"])


def test_build_apply_requests_for_item(_t: Test):
    result = build_apply_requests(
        "Item", "item_1", ["variant_1", "variant_1", "variant_2"]
    )

    assert_eq(
        result,
        [
            {
                "entity_name": "Variant",
                "entity_id": "variant_1",
                "data": {"add_items": ["item_1"]},
            },
            {
                "entity_name": "Variant",
                "entity_id": "variant_2",
                "data": {"add_items": ["item_1"]},
            },
        ],
    )


def test_build_apply_requests_for_variant(_t: Test):
    result = build_apply_requests(
        "Variant", "variant_1", ["component_1", "component_1", "component_2"]
    )

    assert_eq(
        result,
        [
            {
                "entity_name": "Variant",
                "entity_id": "variant_1",
                "data": {
                    "add_components": [
                        {"id": "component_1"},
                        {"id": "component_2"},
                    ]
                },
            }
        ],
    )


def test_fetch_context_entity_requires_found_entity(_t: Test):
    class MissingClient:
        def get_item_for_review(self, *, _id: str):
            return SimpleNamespace(item=None)

    assert_raises(
        ValueError,
        fetch_context_entity,
        entity_id="missing-item",
        entity_name="Item",
        fetch_fn=MissingClient().get_item_for_review,
        result_attr="item",
    )


def test_fetch_context_schema_requires_schema(_t: Test):
    class MissingSchemaClient:
        def get_variant_schema(self):
            return SimpleNamespace(variant_schema=None)

    assert_raises(
        ValueError,
        fetch_context_schema,
        entity_name="Variant",
        schema_mode="update",
        fetch_fn=MissingSchemaClient().get_variant_schema,
        schema_attr="variant_schema",
    )


def test_item_context_raises_on_fetch_error(_t: Test):
    class BrokenClient:
        def get_item_for_review(self, *, _id: str):
            raise RuntimeError("boom")

        def get_item_schema(self):
            return SimpleNamespace(
                item_schema=SimpleNamespace(
                    create=SimpleNamespace(schema_={}),
                    update=SimpleNamespace(schema_={}),
                )
            )

    with patch("f.context.item_context.api_connect", return_value=(BrokenClient(), {})):
        assert_raises(ValueError, item_context_main, entity_id="item_1")


def test_fallback_target_ids_uses_exact_name_matches(_t: Test):
    class FakeNode:
        def __init__(self, data: dict[str, Any]):
            self._data: dict[str, Any] = {}
            self._data = data

        def model_dump(self, exclude: set[str] | None = None) -> dict[str, Any]:
            if not exclude:
                return dict(self._data)
            return {k: v for k, v in self._data.items() if k not in exclude}

    class FakeClient:
        def __init__(self, result: Any):
            self._result: Any = None
            self._result = result

        def search(
            self, _query: str, _types: list[SearchType], _limit: int
        ) -> SimpleNamespace:
            return self._result

    ctx = EntityContext(
        entity_name="Item",
        entity_id="item_1",
        entity_data={"name": "Tomato Paste", "variants": [{"id": "existing_variant"}]},
        related_data={},
        prompt_hints="",
    )

    fake_result = SimpleNamespace(
        search=SimpleNamespace(
            nodes=[
                FakeNode({"id": "variant_a", "name": "Tomato Paste", "desc": None}),
                FakeNode(
                    {
                        "id": "existing_variant",
                        "name": "Tomato Paste",
                        "desc": None,
                    }
                ),
                FakeNode({"id": "variant_b", "name": "tomato paste", "desc": None}),
                FakeNode({"id": "variant_c", "name": "Tomatoes paste", "desc": None}),
            ]
        )
    )

    fake_client = FakeClient(fake_result)
    with patch("f.changes.ai_associate.api_connect", return_value=(fake_client, {})):
        result = fallback_target_ids(ctx, SearchType.VARIANT, "variants")

    assert_eq(result, ["variant_a", "variant_b"])


def main() -> dict[str, object]:
    ensure_test_workspace()
    suite = TestSuite("changes_and_context")
    suite.run(test_variant_context)
    suite.run(test_variant_context_suggest_mode)
    suite.run(test_category_context)
    suite.run(test_item_context)
    suite.run(test_generic_context)
    suite.run(test_existing_association_ids)
    suite.run(test_build_apply_requests_for_item)
    suite.run(test_build_apply_requests_for_variant)
    suite.run(test_fetch_context_entity_requires_found_entity)
    suite.run(test_fetch_context_schema_requires_schema)
    suite.run(test_item_context_raises_on_fetch_error)
    suite.run(test_fallback_target_ids_uses_exact_name_matches)
    return suite.results()
