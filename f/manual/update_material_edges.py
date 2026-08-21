# requirements: project

import json
from typing import Any

import networkx as nx
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine

TREE_INSERT_CHUNK_SIZE = 1000


def main(
    material_id: str,
    user_id: str,
    add_parent_ids: list[str] | None = None,
    remove_parent_ids: list[str] | None = None,
    add_child_ids: list[str] | None = None,
    remove_child_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Adds or removes parent/child edges for an existing material.
    Updates public.material_edges and recomputes the affected entries in
    public.material_tree. Records the change in public.material_history.
    Validates the resulting graph is a DAG and weakly connected before committing.
    """
    add_parent_ids = add_parent_ids or []
    remove_parent_ids = remove_parent_ids or []
    add_child_ids = add_child_ids or []
    remove_child_ids = remove_child_ids or []

    if not any([add_parent_ids, remove_parent_ids, add_child_ids, remove_child_ids]):
        raise ValueError(
            "At least one of add/remove parent/child IDs must be provided."
        )

    engine = create_sql_engine()

    with engine.connect() as conn:
        node_rows = conn.execute(text("SELECT id FROM public.materials")).fetchall()
        edge_rows = conn.execute(
            text(
                "SELECT ancestor_id, descendant_id FROM public.material_tree WHERE depth = 1"
            )
        ).fetchall()
        current_parent_rows = conn.execute(
            text(
                "SELECT parent_id FROM public.material_edges WHERE child_id = :material_id"
            ),
            {"material_id": material_id},
        ).fetchall()
        current_child_rows = conn.execute(
            text(
                "SELECT child_id FROM public.material_edges WHERE parent_id = :material_id"
            ),
            {"material_id": material_id},
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}

    if material_id not in all_node_ids:
        raise ValueError(f"Material {material_id!r} not found.")

    current_parent_ids = {row[0] for row in current_parent_rows}
    current_child_ids = {row[0] for row in current_child_rows}

    # Validate referenced IDs exist
    all_referenced = (
        add_parent_ids + remove_parent_ids + add_child_ids + remove_child_ids
    )
    missing = [mid for mid in all_referenced if mid not in all_node_ids]
    if missing:
        raise ValueError(f"Material IDs not found: {missing}")

    # Validate removes reference actual existing edges
    bad_parent_removes = [p for p in remove_parent_ids if p not in current_parent_ids]
    if bad_parent_removes:
        raise ValueError(
            f"These are not current parents of {material_id!r}: {bad_parent_removes}"
        )
    bad_child_removes = [c for c in remove_child_ids if c not in current_child_ids]
    if bad_child_removes:
        raise ValueError(
            f"These are not current children of {material_id!r}: {bad_child_removes}"
        )

    # Build graph from existing tree (depth=1 edges)
    graph = nx.DiGraph()
    graph.add_nodes_from(all_node_ids)
    for row in edge_rows:
        graph.add_edge(row[0], row[1])

    # Apply edge changes in-memory
    for parent_id in remove_parent_ids:
        graph.remove_edge(parent_id, material_id)
    for child_id in remove_child_ids:
        graph.remove_edge(material_id, child_id)
    for parent_id in add_parent_ids:
        graph.add_edge(parent_id, material_id)
    for child_id in add_child_ids:
        graph.add_edge(material_id, child_id)

    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError(
            "This change would introduce a cycle in the material hierarchy."
        )
    if not nx.is_weakly_connected(graph):
        raise ValueError("This change would result in a disconnected material tree.")

    # Recompute the full closure over the whole graph. A change anywhere can
    # create an undirected shortcut that alters shortest-path depths between
    # unrelated nodes elsewhere in the DAG, so depth cannot be bounded to the
    # edited node's own ancestor/descendant cross-product.
    ugraph = graph.to_undirected()
    tree_entries: list[tuple[str, str, int]] = []
    for node in graph.nodes:
        for d in nx.descendants(graph, node):
            tree_entries.append((node, d, nx.shortest_path_length(ugraph, node, d)))

    original_json = json.dumps(
        {
            "parent_ids": sorted(current_parent_ids),
            "child_ids": sorted(current_child_ids),
        }
    )
    changes_json = json.dumps(
        {
            "add_parent_ids": add_parent_ids,
            "remove_parent_ids": remove_parent_ids,
            "add_child_ids": add_child_ids,
            "remove_child_ids": remove_child_ids,
        }
    )

    with engine.begin() as conn:
        # Update material_edges
        for parent_id in remove_parent_ids:
            conn.execute(
                text(
                    "DELETE FROM public.material_edges WHERE parent_id = :parent_id AND child_id = :child_id"
                ),
                {"parent_id": parent_id, "child_id": material_id},
            )
        for child_id in remove_child_ids:
            conn.execute(
                text(
                    "DELETE FROM public.material_edges WHERE parent_id = :parent_id AND child_id = :child_id"
                ),
                {"parent_id": material_id, "child_id": child_id},
            )
        for parent_id in add_parent_ids:
            conn.execute(
                text(
                    "INSERT INTO public.material_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": parent_id, "child_id": material_id},
            )
        for child_id in add_child_ids:
            conn.execute(
                text(
                    "INSERT INTO public.material_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": material_id, "child_id": child_id},
            )

        # Full rebuild: clear and reinsert the whole closure so depths stay
        # correct even where the change had ripple effects far from material_id.
        # Inserted in chunks of multi-row VALUES to keep this to a handful of
        # round trips instead of one per row.
        conn.execute(text("DELETE FROM public.material_tree"))
        for i in range(0, len(tree_entries), TREE_INSERT_CHUNK_SIZE):
            chunk = tree_entries[i : i + TREE_INSERT_CHUNK_SIZE]
            placeholders = ", ".join(
                f"(:a{j}, :d{j}, :dep{j})" for j in range(len(chunk))
            )
            params: dict[str, str | int] = {}
            for j, (ancestor_id, descendant_id, depth) in enumerate(chunk):
                params[f"a{j}"] = ancestor_id
                params[f"d{j}"] = descendant_id
                params[f"dep{j}"] = depth
            conn.execute(
                text(
                    f"INSERT INTO public.material_tree (ancestor_id, descendant_id, depth) VALUES {placeholders}"
                ),
                params,
            )

        conn.execute(
            text(
                """
                INSERT INTO public.material_history (material_id, datetime, user_id, original, changes)
                VALUES (:material_id, NOW(), :user_id, :original, :changes)
                """
            ),
            {
                "material_id": material_id,
                "user_id": user_id,
                "original": original_json,
                "changes": changes_json,
            },
        )

    print(f"Updated edges for material {material_id!r}")
    wmill.run_script_by_path_async(
        "f/search/materials/index_materials", args={"keys": [material_id]}
    )
    return {"id": material_id}
