# requirements: project

"""
Integration tests for f/utils/* scripts.
Tests: DB connection, S3Client, Meilisearch connection, api_connect,
       general utilities, lang, git.
"""

from sqlalchemy import text

from f.test.framework import Test, TestSuite, assert_true, assert_eq
from f.test.cleanup import ensure_test_workspace


def test_crdb_connection(t: Test):
    """Test that we can connect to CockroachDB and execute a query."""
    from f.utils.db.crdb import create_sql_engine

    engine = create_sql_engine()
    with engine.begin() as conn:
        result = conn.execute(text("SELECT 1"))
        row = result.fetchone()
        assert_eq(row[0], 1, "SELECT 1 should return 1")


def test_crdb_polars_uri(t: Test):
    """Test that Polars URI is generated correctly."""
    from f.utils.db.crdb import create_polars_uri

    uri = create_polars_uri()
    assert_true(uri.startswith("postgresql://"), "Polars URI should start with postgresql://")


def test_crdb_test_table(t: Test):
    """Test that we can create and use a databot.test_* table."""
    from f.utils.db.crdb import create_sql_engine

    engine = create_sql_engine()
    t.cleanup.track_test_table("test_integration_check")

    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS databot.test_integration_check "
            "(id STRING PRIMARY KEY, value STRING)"
        ))
        conn.execute(text(
            "INSERT INTO databot.test_integration_check (id, value) "
            "VALUES ('test1', 'hello')"
        ))
        result = conn.execute(text(
            "SELECT value FROM databot.test_integration_check WHERE id = 'test1'"
        ))
        row = result.fetchone()
        assert_eq(row[0], "hello")


def test_s3_client_exists(t: Test):
    """Test S3Client instantiation and exists check."""
    from f.utils.s3 import S3Client

    s3 = S3Client()
    assert_true(not s3.s3_exists("__test/nonexistent_file_12345.txt"),
                "Nonexistent file should not exist")


def test_s3_client_upload_download(t: Test):
    """Test S3Client upload and download with __test/ prefix."""
    from f.utils.s3 import S3Client

    s3 = S3Client()
    test_path = "__test/integration_test_file.txt"
    t.cleanup.track_s3_path(test_path)

    # Upload
    content = b"integration test content\n"
    import tempfile, os
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

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
            assert_eq(downloaded, content, "Downloaded content should match uploaded content")
    finally:
        os.unlink(tmp_path)
        if os.path.exists(tmp2_path):
            os.unlink(tmp2_path)


def test_meilisearch_connection(t: Test):
    """Test Meilisearch connection."""
    from f.utils.db.meili import meili_connect

    meili = meili_connect()
    assert_true(meili.is_healthy(), "Meilisearch should be healthy")


def test_api_connect(t: Test):
    """Test API connection."""
    from f.utils.api import api_connect

    client, user = api_connect()
    assert_true(user is not None, "User should not be None")
    assert_true("id" in user, "User should have an id")


def test_slugify(t: Test):
    """Test slugify utility."""
    from f.utils.general import slugify

    assert_eq(slugify("Hello World"), "hello-world")
    assert_eq(slugify("  Some Brand™  "), "some-brand")
    assert_eq(slugify("café-latte"), "caf-latte")
    assert_eq(slugify("Multiple   Spaces"), "multiple-spaces")


def test_is_production(t: Test):
    """Test is_production in test workspace."""
    from f.utils.general import is_production

    # In a test workspace, this should always be False
    assert_true(not is_production(), "Should not be production in test workspace")


def test_check_lang(t: Test):
    """Test language validation."""
    from f.search.index_script import check_lang

    assert_eq(check_lang("en"), "en")
    assert_eq(check_lang("xx"), "xx")
    assert_eq(check_lang("sv"), "sv")
    result = check_lang("invalidlang12345")
    assert_true(result is None, "Invalid language should return None")


def test_git_checkout(t: Test):
    """Test git repo checkout."""
    from f.utils.git import checkout_repo
    import shutil

    path = checkout_repo(branch="dev")
    assert_true(path.exists(), "Cloned repo path should exist")
    assert_true((path / "pyproject.toml").exists(), "Should contain pyproject.toml")
    # Clean up cloned repo
    shutil.rmtree(path, ignore_errors=True)


def main() -> dict:
    ensure_test_workspace()
    suite = TestSuite("utils")
    suite.run(test_crdb_connection)
    suite.run(test_crdb_polars_uri)
    suite.run(test_crdb_test_table)
    suite.run(test_s3_client_exists)
    suite.run(test_s3_client_upload_download)
    suite.run(test_meilisearch_connection)
    suite.run(test_api_connect)
    suite.run(test_slugify)
    suite.run(test_is_production)
    suite.run(test_check_lang)
    suite.run(test_git_checkout)
    return suite.results()
