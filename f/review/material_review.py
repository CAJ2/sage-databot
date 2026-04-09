# requirements: project

from typing import Any

import networkx as nx
from sqlalchemy import text

from f.context.context_types import EntityContext
from f.utils.db.crdb import create_sql_engine


def main(
    edit_id: str,
    create_changes: dict[str, Any] | None = None,
    update_changes: dict[str, Any] | None = None,
    entity_id: str | None = None,
) -> dict[str, Any]:
    """
    Validates material tree integrity for changes that modify the `parents` field.
    Queries public.material_tree (depth=1 edges) and constructs an in-memory
    NetworkX DiGraph to verify DAG and weak-connectivity properties.
    Returns a serialized EntityContext dict; populates error_details on failure.
    """
    prompt_hints = (
        "When reviewing changes to a Material, consider:\n"
        "- Whether the material name/description is accurate and specific.\n"
        "- Whether the parent materials form a coherent classification hierarchy.\n"
        "- Whether the material is placed at an appropriate level of specificity.\n"
        "- Avoid circular or redundant parent assignments."
    )

    parents_changed = (create_changes and "parents" in create_changes) or (
        update_changes and "parents" in update_changes
    )

    error_details: str | None = None

    if parents_changed:
        error_details = _validate_material_tree(
            create_changes=create_changes,
            update_changes=update_changes,
            entity_id=entity_id,
        )

    return EntityContext(
        entity_name="Material",
        entity_id=entity_id,
        entity_data={},
        related_data={},
        prompt_hints=prompt_hints,
        error_details=error_details,
    ).model_dump()


def _validate_material_tree(
    create_changes: dict[str, Any] | None,
    update_changes: dict[str, Any] | None,
    entity_id: str | None,
) -> str | None:
    """
    Builds an in-memory graph from the current material_tree, simulates the
    proposed change, and checks DAG + weak-connectivity. Returns an error
    string on failure, or None if the graph is valid.
    """
    engine = create_sql_engine()

    with engine.connect() as conn:
        node_rows = conn.execute(text("SELECT id FROM public.materials")).fetchall()
        edge_rows = conn.execute(
            text(
                "SELECT ancestor_id, descendant_id FROM public.material_tree WHERE depth = 1"
            )
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}
    edges: list[tuple[str, str]] = [(row[0], row[1]) for row in edge_rows]

    graph = nx.DiGraph()
    graph.add_nodes_from(all_node_ids)
    graph.add_edges_from(edges)

    if update_changes and entity_id:
        new_parents: list[str] = update_changes.get("parents", [])
        inbound = list(graph.in_edges(entity_id))
        graph.remove_edges_from(inbound)
        for parent_id in new_parents:
            graph.add_edge(parent_id, entity_id)

    elif create_changes:
        new_node_id = f"__new__{entity_id or 'unknown'}"
        new_parents = create_changes.get("parents", [])
        graph.add_node(new_node_id)
        for parent_id in new_parents:
            graph.add_edge(parent_id, new_node_id)

    if not nx.is_directed_acyclic_graph(graph):
        return (
            "Material tree validation failed: the proposed parent assignment "
            "would introduce a cycle in the material hierarchy."
        )

    if not nx.is_weakly_connected(graph):
        return (
            "Material tree validation failed: the proposed change would result "
            "in a disconnected material tree (not all nodes reachable from root)."
        )

    return None
