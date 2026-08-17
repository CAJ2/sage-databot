# requirements: project

import json
from datetime import datetime, timezone
from typing import Any

import nanoid
import wmill
from mistralai import Mistral
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from f.db.databot.model import BatchJob, Prompt, ensure_batch_job_tables
from f.db.sage.model import SourceContent, SourcePromptResult
from f.graphql.api_client.input_types import UpdateSourceInput
from f.sources.analysis.image_analysis_shared import ImageAnalysis
from f.utils.api import api_connect
from f.utils.db.crdb import create_sql_engine

TERMINAL_STATUSES = {"SUCCESS", "FAILED", "TIMEOUT_EXCEEDED", "CANCELLED"}


def merge_prompt_result(
    existing: SourceContent | None, result: SourcePromptResult
) -> dict[str, Any]:
    existing_dict = existing.model_dump() if existing else {}
    prompts = list(existing_dict.get("prompts") or [])
    prompts.append(result.model_dump())
    existing_dict["prompts"] = prompts
    return {k: v for k, v in existing_dict.items() if v is not None}


def collect_job(
    crdb: Engine,
    client: Any,
    mistral: Mistral,
    job_row: BatchJob,
) -> dict[str, Any]:
    remote_job = mistral.batch.jobs.get(job_id=job_row.id)

    now = datetime.now(timezone.utc)
    with Session(crdb) as session:
        row = session.get(BatchJob, job_row.id)
        assert row is not None
        row.status = remote_job.status
        if remote_job.status in TERMINAL_STATUSES:
            row.completed_at = now
        session.commit()

    if remote_job.status != "SUCCESS":
        print(f"Job {job_row.id} status is {remote_job.status}, nothing to collect")
        return {"status": remote_job.status, "updated": 0}

    if not remote_job.output_file:
        print(f"Job {job_row.id} succeeded but has no output file")
        return {"status": remote_job.status, "updated": 0}

    with Session(crdb) as session:
        prompt = session.get(Prompt, job_row.prompt_id)
        assert prompt is not None
        prompt_model = prompt.model

    response = mistral.files.download(file_id=remote_job.output_file)
    output_bytes = response.read()

    updated = 0
    for line in output_bytes.decode().splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        source_id = record.get("custom_id")
        if not source_id:
            continue

        try:
            body = record["response"]["body"]
            message_content = body["choices"][0]["message"]["content"]
            analysis = ImageAnalysis.model_validate_json(message_content)
        except Exception as e:
            print(f"Failed to parse result for source {source_id}: {e}")
            continue

        existing = client.get_source(source_id)
        if not existing.source:
            print(f"Source {source_id} not found, skipping")
            continue

        existing_content = (
            SourceContent.model_validate(existing.source.content)
            if existing.source.content
            else None
        )

        result = SourcePromptResult(
            id=nanoid.generate(),
            prompt_id=job_row.prompt_id,
            model=prompt_model,
            output=analysis.model_dump(),
            created_at=now.isoformat(),
        )
        merged_content = merge_prompt_result(existing_content, result)

        client.update_source(UpdateSourceInput(id=source_id, content=merged_content))
        updated += 1

    print(f"Job {job_row.id}: updated {updated} source(s)")
    return {"status": remote_job.status, "updated": updated}


def collect_batches(
    crdb: Engine,
    client: Any,
    mistral: Mistral,
) -> dict[str, dict[str, Any]]:
    ensure_batch_job_tables(crdb)

    with Session(crdb) as session:
        stmt = select(BatchJob).where(BatchJob.status.not_in(TERMINAL_STATUSES))
        pending_jobs = list(session.scalars(stmt).all())
        for job in pending_jobs:
            session.expunge(job)

    summary: dict[str, dict[str, Any]] = {}
    for job_row in pending_jobs:
        summary[job_row.id] = collect_job(crdb, client, mistral, job_row)

    return summary


def main(job_id: str | None = None):
    crdb = create_sql_engine()
    client, _ = api_connect()
    api_key = wmill.get_variable("f/api_config/api_mistral_key")
    mistral = Mistral(api_key=api_key)

    if job_id is not None:
        with Session(crdb) as session:
            job_row = session.get(BatchJob, job_id)
            assert job_row is not None
            session.expunge(job_row)
        result = collect_job(crdb, client, mistral, job_row)
        return {**result, "done": result["status"] in TERMINAL_STATUSES}

    return collect_batches(crdb, client, mistral)
