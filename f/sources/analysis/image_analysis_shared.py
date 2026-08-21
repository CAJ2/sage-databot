# requirements: project

from typing import Any

import jinja2
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from f.db.databot.model import Prompt

PROMPT_NAME = "image_analysis"
PROMPT_MODEL = "mistral-small-latest"


class ImageAnalysis(BaseModel):
    item: str
    brand: str | None = None
    marketing_labels: list[str] = []
    certification_labels: list[str] = []
    functional: list[str] = []
    packaging: list[str] = []
    materials: list[str] = []


def latest_prompt(session: Session, name: str = PROMPT_NAME) -> Prompt:
    stmt = (
        select(Prompt)
        .where(Prompt.name == name)
        .order_by(Prompt.created_at.desc())
        .limit(1)
    )
    prompt = session.scalars(stmt).first()
    if prompt is None:
        raise ValueError(
            f"No prompt found with name {name!r}; run f/prompts/sync_prompts first"
        )
    return prompt


def build_batch_request(
    source_id: str, image_url: str, prompt: Prompt, context: str | None = None
) -> dict[str, Any]:
    system_content = jinja2.Template(prompt.content).render(context=context)
    return {
        "custom_id": source_id,
        "body": {
            "model": prompt.model,
            "messages": [
                {"role": "system", "content": system_content},
                {
                    "role": "user",
                    "content": [{"type": "image_url", "image_url": image_url}],
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "image_analysis",
                    "schema": ImageAnalysis.model_json_schema(),
                    "strict": True,
                },
            },
        },
    }
