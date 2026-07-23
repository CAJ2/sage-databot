# requirements: project

import hashlib
import json

from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models import Model

_SYSTEM_PROMPT = (
    "You are a data quality scorer for a sustainability and product database. "
    "Return only a JSON object with a single 'score' field (float 0.0–1.0)."
)

_CRITERIA: dict[str, str] = {
    "items": (
        "An item is a generic product type (e.g. 'Organic Cotton T-Shirt', 'Stainless Steel Water Bottle') — "
        "a category of product, not a specific brand or SKU. "
        "Score this item (0.0–1.0) on: "
        "(1) how common/searchable it is as a staple product, "
        "(2) completeness of name and description, "
        "(3) professionalism."
    ),
    "components": (
        "A component is a physical part that makes up a product "
        "(e.g. 'Glass Bottle', 'Plastic Bag', 'Food'). "
        "Score this component (0.0–1.0) on: "
        "(1) how recognizable it is as a material or component, "
        "(2) completeness of name and description, "
        "(3) professionalism and accuracy."
    ),
    "orgs": (
        "An org is a company, brand, non-profit, certification body, or other organisation "
        "relevant to sustainability or products (e.g. 'Patagonia', 'Fair Trade USA', 'IKEA'). "
        "Score this org (0.0–1.0) on: "
        "(1) how well-known and significant it is, "
        "(2) clarity of what it does, "
        "(3) professionalism."
    ),
    "places": (
        "A place is a physical location such as a recycling center, store, or facility "
        "associated with the circular economy or retail (e.g. 'Recology Sonoma Marin', 'Staples', 'Waste Bin'). "
        "Score this place (0.0–1.0) on: "
        "(1) how notable the location is, "
        "(2) completeness of name and description, "
        "(3) professionalism."
    ),
    "variants": (
        "A variant is a specific, purchasable product — a concrete item with a defined "
        "brand (e.g. 'Organic Dark Chocolate with Cocoa Nibs', 'Mediterranean Hot Pepper and Garlic Tzatziki'). "
        "Score this variant (0.0–1.0) on: "
        "(1) how complete and unambiguous the variant identification is (name, desc), "
        "(2) completeness, "
        "(3) professionalism."
    ),
    "programs": (
        "A program is a sustainability certification, take-back scheme, recycling initiative, "
        "or similar structured programme offered by an org or government body "
        "(e.g. 'Recology', 'H&M Garment Collecting', 'EU Ecolabel'). "
        "Score this program (0.0–1.0) on: "
        "(1) how likely a typical person would use this program, "
        "(2) clarity of what it does and who it is for, "
        "(3) professionalism."
    ),
    "processes": (
        "A process is a specific treatment, manufacturing step, or end-of-life action applied to "
        "a material or variant (e.g. 'Recycle at Home', 'Donate Textiles', 'Compost'). "
        "Score this process (0.0–1.0) on: "
        "(1) how clearly the name and description summarize the intended process or action, "
        "(2) completeness, "
        "(3) precision."
    ),
}


class LLMScore(BaseModel):
    score: float = Field(ge=0.0, le=1.0)


def compute_llm_hash(text_fields: dict[str, str | None]) -> str:
    """SHA256 of sorted key=value pairs, first 16 hex chars."""
    content = json.dumps(text_fields, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _build_prompt(entity_type: str, text_fields: dict[str, str | None]) -> str:
    fields_str = "\n".join(f"{k}: {v or '(empty)'}" for k, v in text_fields.items())
    criteria = _CRITERIA[entity_type]
    return f"Entity type: {entity_type}\n\n{fields_str}\n\n{criteria}"


def score_entity(
    entity_type: str, text_fields: dict[str, str | None], model: Model
) -> float:
    """Prompt LLM to score entity; returns float 0–1."""
    prompt = _build_prompt(entity_type, text_fields)
    agent = Agent(model, output_type=LLMScore, system_prompt=_SYSTEM_PROMPT)
    result = agent.run_sync(prompt)
    return result.output.score
