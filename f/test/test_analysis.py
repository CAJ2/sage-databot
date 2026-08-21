# requirements: project

"""
Integration tests for f/prompts/sync_prompts.py and f/sources/analysis/*.
Tests: prompt sync insert/no-op-on-unchanged, and submit_batch's behavior
given an explicit list of source ids.
"""

import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from mistralai import Mistral
from sqlalchemy import text

from f.db.databot.model import ensure_prompt_tables
from f.prompts.sync_prompts import sync_prompts
from f.sources.analysis.image_analysis_submit import submit_batch
from f.test.cleanup import ensure_test_workspace
from f.test.framework import Test, TestSuite, assert_eq, assert_true
from f.utils.db.crdb import create_sql_engine


def _write_prompt_file(dir_path: Path, name: str, content: str) -> None:
    (dir_path / f"{name}.jinja").write_text(content)


def _fake_upload(**_kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(id="__test_file")


def _make_capturing_upload(calls: list[dict[str, Any]]) -> Any:
    def _upload(**kwargs: Any) -> SimpleNamespace:
        calls.append(kwargs)
        return SimpleNamespace(id="__test_file")

    return _upload


def _make_fake_job_create(job_id: str) -> Any:
    def _create(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(id=job_id, status="QUEUED")

    return _create


def test_sync_prompts_inserts_new_prompt(_t: Test):
    """A new prompt template file is inserted into databot.prompts."""
    engine = create_sql_engine()
    ensure_prompt_tables(engine)

    with tempfile.TemporaryDirectory() as tmp:
        prompts_dir = Path(tmp)
        content = f"test prompt content {uuid.uuid4()}"
        _write_prompt_file(prompts_dir, "image_analysis", content)

        try:
            inserted = sync_prompts(engine, prompts_dir)
            assert_eq(len(inserted), 1, "Should insert exactly one new prompt")

            with engine.begin() as conn:
                row = conn.execute(
                    text("SELECT name, content FROM databot.prompts WHERE id = :id"),
                    {"id": inserted[0]},
                ).fetchone()
            assert row is not None
            assert_eq(row[0], "image_analysis")
            assert_eq(row[1], content)

            # Re-running with the same file content should be a no-op
            inserted_again = sync_prompts(engine, prompts_dir)
            assert_eq(
                len(inserted_again),
                0,
                "Re-sync with unchanged content should insert nothing",
            )
        finally:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM databot.prompts WHERE name = 'image_analysis' AND content = :content"
                    ),
                    {"content": content},
                )


def test_submit_batch_no_candidates(_t: Test):
    """submit_batch returns an empty submission without calling Mistral when given no source ids."""
    ensure_test_workspace()
    engine = create_sql_engine()
    ensure_prompt_tables(engine)

    prompt_id = f"__test_prompt_empty_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO databot.prompts (id, name, model, content, created_at) VALUES (:id, 'image_analysis', 'mistral-small-latest', 'x', now())"
            ),
            {"id": prompt_id},
        )

    def _unexpected_call(*_args: Any, **_kwargs: Any):
        raise AssertionError("Mistral API should not be called with no source ids")

    fake_mistral = cast(
        Mistral,
        cast(
            object,
            SimpleNamespace(
                files=SimpleNamespace(upload=_unexpected_call),
                batch=SimpleNamespace(jobs=SimpleNamespace(create=_unexpected_call)),
            ),
        ),
    )

    try:
        result = submit_batch(engine, fake_mistral, source_ids=[])
        assert_eq(result["submitted"], [])
        assert_eq(result["job_id"], None)
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM databot.prompts WHERE id = :id"), {"id": prompt_id}
            )


def test_submit_batch_submits_given_source_ids(t: Test):
    """submit_batch submits exactly the sources it's given and records a BatchJob."""
    ensure_test_workspace()
    engine = create_sql_engine()
    ensure_prompt_tables(engine)

    prompt_id = f"__test_prompt_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO databot.prompts (id, name, model, content, created_at) VALUES (:id, 'image_analysis', 'mistral-small-latest', 'x', now())"
            ),
            {"id": prompt_id},
        )

    included_id = f"__test_src_included_{uuid.uuid4().hex[:8]}"
    excluded_id = f"__test_src_excluded_{uuid.uuid4().hex[:8]}"
    t.cleanup.track_entity("sources", included_id)
    t.cleanup.track_entity("sources", excluded_id)

    job_id = f"__test_job_{uuid.uuid4().hex[:8]}"
    fake_mistral = cast(
        Mistral,
        cast(
            object,
            SimpleNamespace(
                files=SimpleNamespace(upload=_fake_upload),
                batch=SimpleNamespace(
                    jobs=SimpleNamespace(create=_make_fake_job_create(job_id))
                ),
            ),
        ),
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.sources (id, type, location) VALUES (:id, 'IMAGE', 'https://example.com/included.jpg')"
                ),
                {"id": included_id},
            )
            conn.execute(
                text(
                    "INSERT INTO public.sources (id, type, location) VALUES (:id, 'IMAGE', 'https://example.com/excluded.jpg')"
                ),
                {"id": excluded_id},
            )

        result = submit_batch(engine, fake_mistral, source_ids=[included_id])

        assert_eq(result["job_id"], job_id)
        assert_eq(result["submitted"], [included_id])

        with engine.begin() as conn:
            job_row = conn.execute(
                text("SELECT id FROM databot.batch_jobs WHERE id = :id"),
                {"id": job_id},
            ).fetchone()
            item_ids = conn.execute(
                text(
                    "SELECT source_id FROM databot.batch_job_items WHERE job_id = :id"
                ),
                {"id": job_id},
            ).fetchall()
        assert_true(job_row is not None, "BatchJob row should be created")
        assert_eq([r[0] for r in item_ids], [included_id])
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM databot.batch_job_items WHERE job_id = :id"),
                {"id": job_id},
            )
            conn.execute(
                text("DELETE FROM databot.batch_jobs WHERE id = :id"), {"id": job_id}
            )
            conn.execute(
                text("DELETE FROM databot.prompts WHERE id = :id"), {"id": prompt_id}
            )


def test_submit_batch_normalizes_cdn_source_url(t: Test):
    """submit_batch resolves cdn:// source locations to real URLs before sending to Mistral."""
    ensure_test_workspace()
    engine = create_sql_engine()
    ensure_prompt_tables(engine)

    prompt_id = f"__test_prompt_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO databot.prompts (id, name, model, content, created_at) VALUES (:id, 'image_analysis', 'mistral-small-latest', 'x', now())"
            ),
            {"id": prompt_id},
        )

    source_id = f"__test_src_cdn_{uuid.uuid4().hex[:8]}"
    t.cleanup.track_entity("sources", source_id)

    job_id = f"__test_job_{uuid.uuid4().hex[:8]}"
    upload_calls: list[dict[str, Any]] = []
    fake_mistral = cast(
        Mistral,
        cast(
            object,
            SimpleNamespace(
                files=SimpleNamespace(upload=_make_capturing_upload(upload_calls)),
                batch=SimpleNamespace(
                    jobs=SimpleNamespace(create=_make_fake_job_create(job_id))
                ),
            ),
        ),
    )

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.sources (id, type, location) VALUES (:id, 'IMAGE', 'cdn://sources/off/0000000000000/1.400.jpg')"
                ),
                {"id": source_id},
            )

        submit_batch(engine, fake_mistral, source_ids=[source_id])

        assert_eq(len(upload_calls), 1, "Mistral upload should be called once")
        uploaded_content = upload_calls[0]["file"]["content"].decode()
        assert_true(
            "https://sources.sageleaf.app/off/0000000000000/1.400.jpg"
            in uploaded_content,
            "Uploaded batch request should contain the normalized https URL",
        )
        assert_true(
            "cdn://" not in uploaded_content,
            "Uploaded batch request should not contain the raw cdn:// URL",
        )
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM databot.batch_job_items WHERE job_id = :id"),
                {"id": job_id},
            )
            conn.execute(
                text("DELETE FROM databot.batch_jobs WHERE id = :id"), {"id": job_id}
            )
            conn.execute(
                text("DELETE FROM databot.prompts WHERE id = :id"), {"id": prompt_id}
            )


def main() -> dict[str, object]:
    suite = TestSuite("test_analysis")
    suite.run(test_sync_prompts_inserts_new_prompt)
    suite.run(test_submit_batch_no_candidates)
    suite.run(test_submit_batch_submits_given_source_ids)
    suite.run(test_submit_batch_normalizes_cdn_source_url)
    return suite.results()
