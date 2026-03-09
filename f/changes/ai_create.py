# requirements: project

import json
from typing import Any

from google.genai.types import ThinkingLevel
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModelSettings

from f.changes.ai_suggest import SuggestResult
from f.context.context_types import EntityContext
from f.graphql.api_client.enums import SearchType
from f.utils.api import api_connect
from f.utils.general import llm_agent

_SYSTEM_PROMPT = (
    "You are a data entry expert for a product and sustainability database. "
    "Given a natural language prompt describing a new entity, use the search tool "
    "to look up related entities (categories, orgs, items, etc.) by name to find their IDs, "
    "then suggest all required and relevant field values for creating the new entity. "
    "The 'data' field in your output should be a complete, schema-compliant create mutation payload. "
    "Each field in 'suggestions' should correspond to one create schema field with a value, "
    "confidence (0.0–1.0), and concise reasoning."
)


def main(entity_context: dict[str, Any], prompt: str) -> dict[str, Any]:
    """
    AI agent for creating a new entity from a natural language prompt.
    Accepts a pre-built entity_context (from a context script) containing the create
    schema, then uses a search tool to find related entities and suggests all field values.
    """
    ctx = EntityContext.model_validate(entity_context)
    entity_name = ctx.entity_name
    create_schema = ctx.entity_schema or {}

    context_str = ""
    if ctx.entity_data or ctx.related_data:
        context_str = (
            "\nCONTEXT DATA:\n"
            + json.dumps(
                {"entity": ctx.entity_data, "related": ctx.related_data},
                indent=2,
                default=str,
            )
            + "\n\n"
        )

    hints_str = (
        f"\nMODEL-SPECIFIC GUIDANCE:\n{ctx.prompt_hints}\n\n"
        if ctx.prompt_hints
        else ""
    )

    user_prompt = (
        f"Create a new {entity_name} based on the following description:\n\n"
        f"{prompt}\n\n"
        f"CREATE SCHEMA:\n{json.dumps(create_schema, indent=2, default=str)}\n\n"
        f"{context_str}"
        f"{hints_str}"
        "Use the search tool to find IDs for any referenced entities (categories, orgs, items, etc.). "
        f"Return a SuggestResult with entity_name='{entity_name}', entity_id=None, "
        "a complete 'data' payload ready for the create mutation, and one FieldSuggestion per field."
    )

    agent = Agent(
        llm_agent(),
        output_type=SuggestResult,
        system_prompt=_SYSTEM_PROMPT,
    )

    @agent.tool_plain
    def search(query: str, entity_type: str) -> list[dict[str, Any]]:
        """Search for entities by name. Returns id and all available descriptive fields.
        entity_type: Variant, Item, Component, Category, Org, Place, Region, Material"""
        print(f"Search Tool: query={query}, entity_type={entity_type}")
        try:
            search_type = SearchType[entity_type.upper()]
        except KeyError:
            return []
        c, _ = api_connect()
        result = c.search(query=query, types=[search_type], limit=20)
        print(
            f"Search Result: query={query}, entity_type={entity_type}, nodes={len(result.search.nodes or [])}"
        )
        nodes = [
            n.model_dump(exclude={"typename__"})
            for n in (result.search.nodes or [])
            if n is not None
        ]
        print(f"Search Nodes: {nodes}")
        return nodes

    print("--- AI AGENT ---")
    result = agent.run_sync(
        user_prompt,
        model_settings=GoogleModelSettings(
            google_thinking_config={"thinking_level": ThinkingLevel.LOW}
        ),
    )
    print("--- AI AGENT DONE ---")
    output = result.output

    return SuggestResult(
        entity_name=entity_name,
        entity_id=None,
        data=output.data,
        suggestions=output.suggestions,
    ).model_dump()
