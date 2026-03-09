# requirements: project

import json
from typing import Any

from pydantic_ai import Agent

from f.changes.ai_suggest import SuggestResult
from f.context.context_types import EntityContext
from f.graphql.api_client.enums import SearchType
from f.utils.api import api_connect
from f.utils.general import llm_agent

_SYSTEM_PROMPT = (
    "You are a data management expert for a product and sustainability database. "
    "Given an entity's current data and a reference field to manage, use the search tool "
    "to find relevant entities, then decide what changes to make to the reference field. "
    "For one-to-one ref fields, set the new value directly. "
    "For one-to-many ref fields, suggest which entities to add/change or remove. "
    "The 'data' field in your output should be a schema-compliant mutation payload "
    "(e.g. {'addItems':['<id>'],'removeItems':['<id>']} for a many field, "
    "or {'region':{'id':'<id>'}} for a single ref). "
    "Provide concise reasoning for each suggested operation."
)


def main(
    entity_context: dict[str, Any],
    ref_field: str,
    ref_entity_type: str,
    prompt: str | None = None,
) -> dict[str, Any]:
    """
    AI agent for managing a single reference field on an entity.
    Searches for related entities, then suggests add/remove/change operations.
    """
    ctx = EntityContext.model_validate(entity_context)

    current_ref = ctx.entity_data.get(ref_field)
    schema = ctx.entity_schema or {}
    schema_excerpt = schema.get("properties", {}).get(ref_field, {})

    try:
        search_type = SearchType[ref_entity_type.upper()]
    except KeyError:
        raise ValueError(f"Invalid ref_entity_type: {ref_entity_type}")

    user_prompt = (
        f"Manage the '{ref_field}' reference field for this {ctx.entity_name}.\n\n"
        f"CURRENT ENTITY DATA:\n{json.dumps(ctx.entity_data, indent=2, default=str)}\n\n"
        f"CURRENT VALUE OF '{ref_field}':\n{json.dumps(current_ref, indent=2, default=str)}\n\n"
        f"SCHEMA FOR '{ref_field}':\n{json.dumps(schema_excerpt, indent=2, default=str)}\n\n"
    )
    if prompt:
        user_prompt += f"ADDITIONAL GUIDANCE: {prompt}\n\n"
    user_prompt += (
        "Use the search tool to find relevant entities, then return a SuggestResult with:\n"
        "- 'data': the mutation payload (e.g. {'addItems': ['id1'], 'removeItems': ['id2']})\n"
        "- 'suggestions': one FieldSuggestion per operation with field name, value, confidence, reasoning"
    )

    agent = Agent(
        llm_agent(),
        output_type=SuggestResult,
        system_prompt=_SYSTEM_PROMPT,
    )

    @agent.tool_plain
    def search(query: str) -> list[dict[str, Any]]:
        """Search for matching entities. Returns id and all available descriptive fields."""
        print(f"Search Tool: query={query}, entity_type={ref_entity_type}")
        client, _ = api_connect()
        result = client.search(
            query=query,
            types=[search_type],
            limit=20,
        )
        print(
            f"Search Result: query={query}, entity_type={ref_entity_type}, nodes={len(result.search.nodes or [])}"
        )
        nodes = [
            n.model_dump(exclude={"typename__"})
            for n in (result.search.nodes or [])
            if n is not None
        ]
        print(f"Search Nodes: {nodes}")
        return nodes

    result = agent.run_sync(user_prompt)
    output = result.output

    return SuggestResult(
        entity_name=ctx.entity_name,
        entity_id=ctx.entity_id,
        data=output.data,
        suggestions=output.suggestions,
    ).model_dump()
