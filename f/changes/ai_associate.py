# requirements: project

import json
import re
from typing import Any

from pydantic_ai import Agent
from pydantic_ai.usage import UsageLimits

from f.agents.toolsets import search_fixed_type_toolset
from f.changes.ai_shared import AssociationIDsResult
from f.context.context_types import EntityContext
from f.graphql.api_client.enums import SearchType
from f.utils.api import api_connect
from f.utils.general import llm_agent, llm_model_settings

_SYSTEM_PROMPT = (
    "You are a data association expert for a product and sustainability database. "
    "Given one source record, use the search tool to find existing records that should be "
    "associated to it. Return only the IDs of records that should be newly associated. "
    "Do not invent IDs. Do not return records that are already linked. "
    "If nothing clearly fits, return an empty list."
)


def existing_association_ids(
    entity_data: dict[str, Any], association_field: str
) -> list[str]:
    current_value = entity_data.get(association_field)
    if not isinstance(current_value, list):
        return []

    existing_ids: list[str] = []
    seen: set[str] = set()
    for item in current_value:
        target_id = item.get("id") if isinstance(item, dict) else item
        if not isinstance(target_id, str) or target_id in seen:
            continue
        seen.add(target_id)
        existing_ids.append(target_id)
    return existing_ids


def _normalize_name(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(normalized.split())


def _candidate_name(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None
    name = node.get("name")
    if isinstance(name, str) and name.strip():
        return name
    label = node.get("label")
    if isinstance(label, str) and label.strip():
        return label
    return None


def fallback_target_ids(
    ctx: EntityContext,
    search_type: SearchType,
    association_field: str,
) -> list[str]:
    source_name = ctx.entity_data.get("name")
    if not isinstance(source_name, str) or not source_name.strip():
        return []

    normalized_source = _normalize_name(source_name)
    existing_ids = set(existing_association_ids(ctx.entity_data, association_field))

    client, _ = api_connect()
    result = client.search(query=f'"{source_name}"', types=[search_type], limit=20)

    fallback_ids: list[str] = []
    seen: set[str] = set()
    for node in result.search.nodes or []:
        if node is None:  # pyright: ignore[reportUnnecessaryComparison]
            continue
        dumped = node.model_dump(exclude={"typename__"})
        target_id = dumped.get("id")
        target_name = _candidate_name(dumped)
        if (
            not isinstance(target_id, str)
            or not target_name
            or _normalize_name(target_name) != normalized_source
            or target_id in existing_ids
            or target_id in seen
        ):
            continue
        seen.add(target_id)
        fallback_ids.append(target_id)
    return fallback_ids


def _build_prompt(
    ctx: EntityContext,
    target_entity_type: str,
    association_field: str,
    prompt: str | None = None,
) -> str:
    current_value = ctx.entity_data.get(association_field)
    context_payload = {
        "entity": ctx.entity_data,
        "related": ctx.related_data,
    }
    existing_ids = existing_association_ids(ctx.entity_data, association_field)

    user_prompt = (
        f"Find {target_entity_type} records to associate to this {ctx.entity_name}.\n\n"
        f"SOURCE ENTITY:\n{json.dumps(context_payload, indent=2, default=str)}\n\n"
        f"CURRENT VALUE OF '{association_field}':\n"
        f"{json.dumps(current_value, indent=2, default=str)}\n\n"
        f"ALREADY ASSOCIATED IDS:\n{json.dumps(existing_ids, indent=2)}\n\n"
        f"MODEL-SPECIFIC GUIDANCE:\n{ctx.prompt_hints}\n\n"
    )
    if prompt:
        user_prompt += f"ADDITIONAL GUIDANCE:\n{prompt}\n\n"
    user_prompt += (
        f"Use the search tool to find matching existing {target_entity_type} records. "
        f"Only return IDs for {target_entity_type} records that should be added to "
        f"'{association_field}' on this {ctx.entity_name}. "
        "If search returns exact or clearly equivalent name matches that are not already associated, "
        "return those IDs rather than an empty list. "
        "If multiple candidates share the same clearly matching name, include all of them. "
        "Return exactly one JSON object with this shape: "
        '{"target_ids":["id1","id2"]}.'
    )
    return user_prompt


def main(
    entity_context: dict[str, Any],
    target_entity_type: str,
    association_field: str,
    prompt: str | None = None,
) -> dict[str, Any]:
    ctx = EntityContext.model_validate(entity_context)

    try:
        search_type = SearchType[target_entity_type.upper()]
    except KeyError:
        raise ValueError(f"Invalid target_entity_type: {target_entity_type}")

    agent = Agent(
        llm_agent(),
        output_type=AssociationIDsResult,
        system_prompt=_SYSTEM_PROMPT,
        toolsets=[search_fixed_type_toolset(search_type)],
    )

    result = agent.run_sync(
        _build_prompt(ctx, target_entity_type, association_field, prompt),
        model_settings=llm_model_settings(),
        usage_limits=UsageLimits(input_tokens_limit=16000, output_tokens_limit=1000),
    )
    output = result.output
    if not output.target_ids:
        output.target_ids = fallback_target_ids(ctx, search_type, association_field)
    return output.model_dump()
