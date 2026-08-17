# requirements: project

import json
from datetime import datetime, timezone
from typing import Any

import wmill
from mistralai import Mistral
from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from f.db.databot.model import (
    BatchJob,
    BatchJobItem,
    Prompt,
    ensure_batch_job_tables,
    ensure_prompt_tables,
)
from f.db.sage.model import Source
from f.sources.analysis.image_analysis_shared import (
    PROMPT_NAME,
    build_batch_request,
    latest_prompt,
)
from f.utils.db.crdb import create_sql_engine
from f.utils.urls import normalize_source_url


def submit_batch(
    crdb: Engine,
    mistral: Mistral,
    source_ids: list[str],
) -> dict[str, Any]:
    ensure_prompt_tables(crdb)
    ensure_batch_job_tables(crdb)

    with Session(crdb) as session:
        prompt: Prompt = latest_prompt(session, PROMPT_NAME)
        session.expunge(prompt)

        sources = list(
            session.scalars(select(Source).where(Source.id.in_(source_ids)))
            .unique()
            .all()
        )

    if not sources:
        print("No sources to submit for analysis")
        return {"job_id": None, "prompt_id": prompt.id, "submitted": []}

    print(f"Submitting {len(sources)} source(s) for prompt {prompt.id}")

    lines = [
        build_batch_request(
            source.id,
            normalize_source_url(source.location),
            prompt,
            context=source.content.context if source.content else None,
        )
        for source in sources
        if source.location is not None
    ]
    jsonl_content = "\n".join(json.dumps(line) for line in lines).encode()

    uploaded = mistral.files.upload(
        file={"file_name": "image_analysis_batch.jsonl", "content": jsonl_content},
        purpose="batch",
    )
    job = mistral.batch.jobs.create(
        input_files=[uploaded.id],
        model=prompt.model,
        endpoint="/v1/chat/completions",
    )

    now = datetime.now(timezone.utc)
    submitted_ids = [source.id for source in sources]
    with Session(crdb) as session:
        session.add(
            BatchJob(
                id=job.id,
                prompt_id=prompt.id,
                status=job.status,
                created_at=now,
            )
        )
        for source_id in submitted_ids:
            session.add(BatchJobItem(job_id=job.id, source_id=source_id))
        session.commit()

    print(f"Submitted batch job {job.id} with {len(submitted_ids)} source(s)")

    return {"job_id": job.id, "prompt_id": prompt.id, "submitted": submitted_ids}


def main(source_ids: list[str]):
    crdb = create_sql_engine()
    api_key = wmill.get_variable("f/api_config/api_mistral_key")
    mistral = Mistral(api_key=api_key)
    return submit_batch(crdb, mistral, source_ids)
