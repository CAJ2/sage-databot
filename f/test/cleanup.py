# requirements: project

"""
Cleanup utilities for integration tests.

Tracks entities, tables, S3 objects, and Meilisearch indexes created during
tests and cleans them up afterwards.
"""

import os
from typing import Any

from sqlalchemy import text, event, Engine

import wmill
import meilisearch

from f.utils.db.crdb import create_sql_engine
from f.utils.s3 import S3Client


# Tables in the public schema that may contain __test_ prefixed IDs
PUBLIC_TABLES_WITH_IDS = [
    "public.variants",
    "public.orgs",
    "public.sources",
    "public.items",
    "public.components",
    "public.processes",
    "public.places",
    "public.categories",
    "public.materials",
    "public.changes",
    "public.tags",
]

# Junction/association tables with __test_ prefixed foreign keys
PUBLIC_JUNCTION_TABLES = [
    ("public.variants_sources", "variant_id"),
    ("public.variants_sources", "source_id"),
    ("public.external_sources", "variant_id"),
    ("public.external_sources", "org_id"),
]


class CleanupTracker:
    """
    Tracks resources created during tests for cleanup.

    Usage:
        cleanup = CleanupTracker()
        cleanup.track_entity("variants", "some_id")
        cleanup.track_test_table("test_categories_load")
        cleanup.track_s3_path("__test/some_file.txt")
        cleanup.track_meili_index("test_variants")
        # ... run tests ...
        cleanup.execute()
    """

    def __init__(self):
        self._entities: list[tuple[str, str]] = []  # (table, id)
        self._test_tables: list[str] = []  # databot.test_* table names
        self._s3_paths: list[str] = []  # S3 object paths to delete
        self._meili_indexes: list[str] = []  # Meilisearch index names

    def track_entity(self, table: str, entity_id: str):
        """Track an entity in a public schema table for deletion."""
        if not table.startswith("public."):
            table = f"public.{table}"
        self._entities.append((table, entity_id))

    def track_test_table(self, table_name: str):
        """Track a databot.test_* table for dropping."""
        if not table_name.startswith("databot."):
            table_name = f"databot.{table_name}"
        self._test_tables.append(table_name)

    def track_s3_path(self, path: str):
        """Track an S3 object path for deletion."""
        self._s3_paths.append(path)

    def track_meili_index(self, index_name: str):
        """Track a Meilisearch index for deletion."""
        self._meili_indexes.append(index_name)

    def execute(self):
        """Run all cleanup operations. Logs errors but does not raise."""
        self._cleanup_db()
        self._cleanup_s3()
        self._cleanup_meilisearch()

    def _cleanup_db(self):
        """Clean up DB entities and test tables."""
        if not self._entities and not self._test_tables:
            return

        try:
            engine = create_sql_engine()
        except Exception as e:
            print(f"[cleanup] Failed to connect to DB: {e}")
            return

        # Delete tracked entities from public schema
        for table, entity_id in self._entities:
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(f"DELETE FROM {table} WHERE id = :id"),
                        {"id": entity_id},
                    )
                print(f"[cleanup] Deleted {entity_id} from {table}")
            except Exception as e:
                print(f"[cleanup] Failed to delete {entity_id} from {table}: {e}")

        # Bulk-delete any __test_ prefixed rows (safety net)
        for table in PUBLIC_TABLES_WITH_IDS:
            try:
                with engine.begin() as conn:
                    result = conn.execute(
                        text(f"DELETE FROM {table} WHERE id LIKE :pattern"),
                        {"pattern": "__test_%"},
                    )
                    if result.rowcount > 0:
                        print(
                            f"[cleanup] Deleted {result.rowcount} __test_ rows from {table}"
                        )
            except Exception:
                # Table might not exist, that's fine
                pass

        # Clean up junction tables
        for table, col in PUBLIC_JUNCTION_TABLES:
            try:
                with engine.begin() as conn:
                    result = conn.execute(
                        text(f"DELETE FROM {table} WHERE {col} LIKE :pattern"),
                        {"pattern": "__test_%"},
                    )
                    if result.rowcount > 0:
                        print(
                            f"[cleanup] Deleted {result.rowcount} __test_ rows from {table}.{col}"
                        )
            except Exception:
                pass

        # Drop test tables
        for table in self._test_tables:
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                print(f"[cleanup] Dropped table {table}")
            except Exception as e:
                print(f"[cleanup] Failed to drop {table}: {e}")

        # Discover and drop any remaining databot.test_* tables
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'databot' AND table_name LIKE 'test_%'"
                    )
                )
                discovered = [f"databot.{row[0]}" for row in result]
        except Exception as e:
            print(f"[cleanup] Failed to discover test tables: {e}")
            discovered = []

        for table_name in discovered:
            if table_name not in self._test_tables:
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    print(f"[cleanup] Dropped discovered table {table_name}")
                except Exception as e:
                    print(f"[cleanup] Failed to drop {table_name}: {e}")

    def _cleanup_s3(self):
        """Delete tracked S3 objects and any objects under __test/ prefix."""
        try:
            s3 = S3Client()
        except Exception as e:
            print(f"[cleanup] Failed to connect to S3: {e}")
            return

        # Delete tracked paths
        for path in self._s3_paths:
            try:
                s3.client.delete_object(Bucket=s3.bucket, Key=path)
                print(f"[cleanup] Deleted S3 object {path}")
            except Exception as e:
                print(f"[cleanup] Failed to delete S3 {path}: {e}")

        # Bulk-delete everything under __test/ prefix
        try:
            for page in s3.s3_scan("__test/"):
                for obj in page.get("Contents", []):
                    try:
                        s3.client.delete_object(Bucket=s3.bucket, Key=obj["Key"])
                    except Exception:
                        pass
            print("[cleanup] Cleaned up __test/ S3 prefix")
        except Exception as e:
            print(f"[cleanup] Failed to clean __test/ S3 prefix: {e}")

    def _cleanup_meilisearch(self):
        """Delete tracked Meilisearch indexes."""
        if not self._meili_indexes:
            return

        try:
            meili_res = wmill.get_resource("f/api_config/api_meilisearch")
            if meili_res is None:
                print("[cleanup] No Meilisearch resource found")
                return
            meili = meilisearch.Client(
                str(meili_res.get("api_url", "")),
                api_key=meili_res.get("api_key", None),
            )
        except Exception as e:
            print(f"[cleanup] Failed to connect to Meilisearch: {e}")
            return

        for index_name in self._meili_indexes:
            try:
                meili.index(index_name).delete()
                print(f"[cleanup] Deleted Meilisearch index {index_name}")
            except Exception as e:
                print(f"[cleanup] Failed to delete index {index_name}: {e}")

        # Also clean up any test_ prefixed indexes we didn't track
        try:
            indexes = meili.get_indexes()
            for idx in indexes.get("results", []):
                if idx.uid.startswith("test_") and idx.uid not in self._meili_indexes:
                    try:
                        meili.index(idx.uid).delete()
                        print(f"[cleanup] Deleted discovered index {idx.uid}")
                    except Exception:
                        pass
        except Exception:
            pass


class DBTracker:
    """
    Tracks all INSERT/UPDATE/DELETE/UPSERT operations executed via SQLAlchemy.
    Attach to an engine to monitor what SQL a script executes.

    Usage:
        engine = create_sql_engine()
        tracker = DBTracker(engine)
        # ... run code that uses the engine ...
        tracker.detach(engine)
        print(tracker.operations)
    """

    def __init__(self, engine: Engine):
        self.operations: list[dict[str, str]] = []
        event.listen(engine, "before_cursor_execute", self._on_execute)

    def _on_execute(
        self,
        _conn: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        stmt_upper = statement.strip().upper()
        if stmt_upper.startswith(("INSERT", "UPDATE", "DELETE", "UPSERT")):
            self.operations.append(
                {
                    "statement": statement[:500],  # truncate for readability
                    "type": stmt_upper.split()[0],
                }
            )

    def detach(self, engine: Engine):
        """Stop listening to engine events."""
        event.remove(engine, "before_cursor_execute", self._on_execute)

    def has_write_to(self, table_name: str) -> bool:
        """Check if any tracked operation targets the given table."""
        for op in self.operations:
            if table_name.lower() in op["statement"].lower():
                return True
        return False

    def count_by_type(self, op_type: str) -> int:
        """Count operations of a given type (INSERT, UPDATE, DELETE, UPSERT)."""
        return sum(1 for op in self.operations if op["type"] == op_type.upper())


def ensure_test_workspace():
    """Validate we're running in a test workspace. Raises if not."""
    workspace = os.environ.get("WM_WORKSPACE", "")
    if not workspace.startswith("wm-fork-test"):
        raise RuntimeError(
            f"Safety check failed: workspace is '{workspace}', expected 'wm-fork-test*'. Tests must run in a forked test workspace."
        )
    return workspace


def main():
    """Self-test: verify cleanup tracker can be instantiated and track resources."""
    cleanup = CleanupTracker()
    cleanup.track_entity("variants", "__test_self_test_123")
    cleanup.track_test_table("test_self_test")
    cleanup.track_s3_path("__test/self_test.txt")
    cleanup.track_meili_index("test_self_test")
    return {"status": "ok"}
