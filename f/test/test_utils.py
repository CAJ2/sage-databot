# requirements: project

"""
Integration tests for f/utils/* scripts.
Tests: DB connection, S3Client, Typesense connection, api_connect,
       general utilities, lang, git.
"""

import json
import os
import shutil
import tempfile

from sqlalchemy import text

from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_eq, assert_raises, assert_true
from f.utils.api import api_connect
from f.utils.db.crdb import create_polars_uri, create_sql_engine
from f.utils.db.typesense import check_lang, parse_typesense_nodes, ts_connect
from f.utils.general import is_production, slugify
from f.utils.git import checkout_repo
from f.utils.s3 import S3Client


def test_crdb_connection(_t: Test):
    """Test that we can connect to CockroachDB and execute a query."""
    engine = create_sql_engine()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT 1"))
        row = result.fetchone()
        assert row is not None
        assert_eq(row[0], 1, "SELECT 1 should return 1")


def test_crdb_polars_uri(_t: Test):
    """Test that Polars URI is generated correctly."""
    uri = create_polars_uri()
    assert_true(
        uri.startswith("postgresql://"), "Polars URI should start with postgresql://"
    )


def test_crdb_test_table(t: Test):
    """Test that we can create and use a databot.test_* table."""
    engine = create_sql_engine()
    t.cleanup.track_test_table("test_integration_check")

    with engine.begin() as conn:
        _ = conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS databot.test_integration_check (id STRING PRIMARY KEY, value STRING)"
            )
        )
        _ = conn.execute(
            text(
                "INSERT INTO databot.test_integration_check (id, value) VALUES ('test1', 'hello')"
            )
        )
        result = conn.execute(
            text("SELECT value FROM databot.test_integration_check WHERE id = 'test1'")
        )
        row = result.fetchone()
        assert row is not None
        assert_eq(row[0], "hello")


def test_s3_client_exists(_t: Test):
    """Test S3Client instantiation and exists check."""
    s3 = S3Client()
    assert_true(
        not s3.s3_exists("__test/nonexistent_file_12345.txt"),
        "Nonexistent file should not exist",
    )


def test_s3_client_upload_download(t: Test):
    """Test S3Client upload and download with __test/ prefix."""
    s3 = S3Client()
    test_path = "__test/integration_test_file.txt"
    t.cleanup.track_s3_path(test_path)

    # Upload
    content = b"integration test content\n"
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
        _ = tmp.write(content)
        tmp_path = tmp.name

    tmp2_path = None
    try:
        with open(tmp_path, "rb") as f:
            result = s3.s3_upload(test_path, f)
            assert_true(result, "Upload should succeed")

        assert_true(s3.s3_exists(test_path), "Uploaded file should exist")

        # Download
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp2:
            tmp2_path = tmp2.name

        with open(tmp2_path, "wb") as f:
            result = s3.s3_download(test_path, f)
            assert_true(result, "Download should succeed")

        with open(tmp2_path, "rb") as f:
            downloaded = f.read()
            assert_eq(
                downloaded, content, "Downloaded content should match uploaded content"
            )
    finally:
        os.unlink(tmp_path)
        if tmp2_path is not None and os.path.exists(tmp2_path):
            os.unlink(tmp2_path)


def test_typesense_connection(_t: Test):
    """Test Typesense connection."""
    ts = ts_connect()
    assert_true(ts.operations.is_healthy(), "Typesense should be healthy")


def test_api_connect(_t: Test):
    """Test API connection."""
    _, user = api_connect()
    assert_true(user is not None, "User should not be None")
    assert_true("id" in user, "User should have an id")


def test_slugify(_t: Test):
    """Test slugify utility."""
    assert_eq(slugify("Hello World"), "hello-world")
    assert_eq(slugify("  Some Brand™  "), "some-brand")
    assert_eq(slugify("Café Latte"), "café-latte")
    assert_eq(slugify("Multiple   Spaces"), "multiple-spaces")


def test_is_production(_t: Test):
    """Test is_production in test workspace."""
    # In a test workspace, this should always be False
    assert_true(not is_production(), "Should not be production in test workspace")


def test_check_lang(_t: Test):
    """Test language validation."""
    assert_eq(check_lang("en"), "en")
    assert_eq(check_lang("xx"), "xx")
    assert_eq(check_lang("sv"), "sv")
    result = check_lang("invalidlang12345")
    assert_true(result is None, "Invalid language should return None")


def test_typesense_nodes_variable(_t: Test):
    assert_eq(
        parse_typesense_nodes('[{"host":"localhost","port":"8108","protocol":"http"}]'),
        [{"host": "localhost", "port": "8108", "protocol": "http"}],
        "Typesense nodes should come from the JSON nodes variable",
    )


def test_typesense_nodes_variable_rejects_invalid_json(_t: Test):
    assert_raises(json.JSONDecodeError, parse_typesense_nodes, "localhost:8108")


def test_git_checkout(_t: Test):
    """Test git repo checkout."""

    path = checkout_repo(branch="dev")
    assert_true(path.exists(), "Cloned repo path should exist")
    assert_true((path / "pyproject.toml").exists(), "Should contain pyproject.toml")
    # Clean up cloned repo
    shutil.rmtree(path, ignore_errors=True)


def main() -> dict[str, object]:
    _ = ensure_test_workspace()
    suite = TestSuite("utils")
    suite.run(test_crdb_connection)
    suite.run(test_crdb_polars_uri)
    suite.run(test_crdb_test_table)
    suite.run(test_s3_client_exists)
    suite.run(test_s3_client_upload_download)
    suite.run(test_typesense_connection)
    suite.run(test_api_connect)
    suite.run(test_slugify)
    suite.run(test_is_production)
    suite.run(test_check_lang)
    suite.run(test_typesense_nodes_variable)
    suite.run(test_typesense_nodes_variable_rejects_invalid_json)
    suite.run(test_git_checkout)
    return suite.results()
