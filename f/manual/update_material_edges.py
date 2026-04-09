# requirements: project

import json
from typing import Any

import networkx as nx
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


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

    # Capture affected nodes before applying changes (for cross-product cleanup)
    old_ancestors: set[str] = nx.ancestors(graph, material_id)
    old_descendants: set[str] = nx.descendants(graph, material_id)

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

    ugraph = graph.to_undirected()
    new_ancestors: set[str] = nx.ancestors(graph, material_id)
    new_descendants: set[str] = nx.descendants(graph, material_id)

    # Recompute all tree entries for material_id (as ancestor or descendant)
    new_ancestor_entries = [
        (a, material_id, nx.shortest_path_length(ugraph, a, material_id))
        for a in new_ancestors
    ]
    new_descendant_entries = [
        (material_id, d, nx.shortest_path_length(ugraph, material_id, d))
        for d in new_descendants
    ]

    # Recompute affected cross-product entries: all (A, D) pairs from the union
    # of old and new ancestor/descendant sets. Stale entries are deleted first,
    # then valid paths are reinserted.
    affected_ancestors = old_ancestors | new_ancestors
    affected_descendants = old_descendants | new_descendants
    cross_entries: list[tuple[str, str, float]] = []
    for a in affected_ancestors:
        for d in affected_descendants:
            if nx.has_path(graph, a, d):
                cross_entries.append((a, d, nx.shortest_path_length(ugraph, a, d)))

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

        # Remove all existing tree entries involving material_id
        conn.execute(
            text(
                "DELETE FROM public.material_tree WHERE ancestor_id = :mid OR descendant_id = :mid"
            ),
            {"mid": material_id},
        )

        # Remove stale cross-product entries (A, D) that may have changed
        for a in affected_ancestors:
            for d in affected_descendants:
                conn.execute(
                    text(
                        "DELETE FROM public.material_tree WHERE ancestor_id = :a AND descendant_id = :d"
                    ),
                    {"a": a, "d": d},
                )

        # Reinsert tree entries for material_id and valid cross-product entries
        for ancestor_id, descendant_id, depth in (
            new_ancestor_entries + new_descendant_entries + cross_entries
        ):
            conn.execute(
                text(
                    """
                    INSERT INTO public.material_tree (ancestor_id, descendant_id, depth)
                    VALUES (:ancestor_id, :descendant_id, :depth)
                    ON CONFLICT (ancestor_id, descendant_id) DO UPDATE
                        SET depth = EXCLUDED.depth
                        WHERE public.material_tree.depth > EXCLUDED.depth
                    """
                ),
                {
                    "ancestor_id": ancestor_id,
                    "descendant_id": descendant_id,
                    "depth": depth,
                },
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
