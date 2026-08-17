# requirements: project

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from f.sources.analysis.image_analysis_shared import (
    PROMPT_MODEL as IMAGE_ANALYSIS_PROMPT_MODEL,
)
from f.db.databot.model import Prompt, ensure_prompt_tables
from f.utils.db.crdb import create_sql_engine
from f.utils.git import checkout_repo

PROMPT_MODELS: dict[str, str] = {
    "image_analysis": IMAGE_ANALYSIS_PROMPT_MODEL,
}


def sync_prompts(crdb: Engine, prompts_dir: Path) -> list[str]:
    """
    Sync prompt template files under `prompts_dir` into `databot.prompts`.

    Each file is inserted as a new, content-addressed row if it doesn't
    already exist (same name+model+content always maps to the same id, so
    re-running is a no-op). Existing rows are never updated or deleted.

    Returns the list of newly inserted prompt ids.
    """
    ensure_prompt_tables(crdb)

    inserted_ids: list[str] = []
    now = datetime.now(timezone.utc)

    with Session(crdb) as session:
        for path in sorted(prompts_dir.glob("*.jinja")):
            name = path.stem
            model = PROMPT_MODELS.get(name)
            if model is None:
                print(f"No model mapping for prompt {name!r}, skipping")
                continue

            content = path.read_text()
            prompt_id = hashlib.sha256(f"{model}\n{content}".encode()).hexdigest()[:16]

            if session.get(Prompt, prompt_id) is not None:
                print(f"Prompt {name!r} ({prompt_id}) already exists, skipping")
                continue

            session.add(
                Prompt(
                    id=prompt_id,
                    name=name,
                    model=model,
                    content=content,
                    created_at=now,
                )
            )
            inserted_ids.append(prompt_id)
            print(f"Inserted prompt {name!r} ({prompt_id})")

        session.commit()

    return inserted_ids


def main():
    crdb = create_sql_engine()
    repo = checkout_repo()
    prompts_dir = repo / "src" / "analysis" / "prompts"
    return sync_prompts(crdb, prompts_dir)
