# requirements: project

from typing import Any

from pydantic_ai import FunctionToolset

from f.graphql.api_client.enums import SearchType
from f.utils.api import api_connect


def _run_search(query: str, types: list[SearchType]) -> list[dict[str, Any]]:
    c, _ = api_connect()
    result = c.search(query=query, types=types, limit=20)
    return [
        n.model_dump(exclude={"typename__"})
        for n in (result.search.nodes or [])
        if n is not None  # pyright: ignore[reportUnnecessaryComparison]
    ]


def search_multi_type_toolset() -> FunctionToolset:
    """Toolset with a search tool that accepts entity_type as a parameter (used by ai_create)."""

    def search(query: str, entity_type: str) -> list[dict[str, Any]]:
        """Search for entities by name. Returns id and all available descriptive fields.
        entity_type: Variant, Item, Component, Category, Org, Place, Region, Material"""
        print(f"Search Tool: query={query}, entity_type={entity_type}")
        try:
            search_type = SearchType[entity_type.upper()]
        except KeyError:
            return []
        nodes = _run_search(query, [search_type])
        print(
            f"Search Result: query={query}, entity_type={entity_type}, nodes={len(nodes)}"
        )
        print(f"Search Nodes: {nodes}")
        return nodes

    return FunctionToolset(tools=[search])


def search_fixed_type_toolset(search_type: SearchType) -> FunctionToolset:
    """Toolset with a search tool bound to a specific entity type (used by ai_ref)."""

    def search(query: str) -> list[dict[str, Any]]:
        """Search for matching entities. Returns id and all available descriptive fields."""
        print(f"Search Tool: query={query}, entity_type={search_type.name}")
        nodes = _run_search(query, [search_type])
        print(
            f"Search Result: query={query}, entity_type={search_type.name}, nodes={len(nodes)}"
        )
        print(f"Search Nodes: {nodes}")
        return nodes

    return FunctionToolset(tools=[search])
