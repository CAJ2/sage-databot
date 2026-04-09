# requirements: project

import json
from typing import Any

import networkx as nx
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(
    category_id: str,
    user_id: str,
    add_parent_ids: list[str] | None = None,
    remove_parent_ids: list[str] | None = None,
    add_child_ids: list[str] | None = None,
    remove_child_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Adds or removes parent/child edges for an existing category.
    Updates public.category_edges and recomputes the affected entries in
    public.category_tree. Records the change in public.category_history.
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
        node_rows = conn.execute(text("SELECT id FROM public.categories")).fetchall()
        edge_rows = conn.execute(
            text(
                "SELECT ancestor_id, descendant_id FROM public.category_tree WHERE depth = 1"
            )
        ).fetchall()
        current_parent_rows = conn.execute(
            text(
                "SELECT parent_id FROM public.category_edges WHERE child_id = :category_id"
            ),
            {"category_id": category_id},
        ).fetchall()
        current_child_rows = conn.execute(
            text(
                "SELECT child_id FROM public.category_edges WHERE parent_id = :category_id"
            ),
            {"category_id": category_id},
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}

    if category_id not in all_node_ids:
        raise ValueError(f"Category {category_id!r} not found.")

    current_parent_ids = {row[0] for row in current_parent_rows}
    current_child_ids = {row[0] for row in current_child_rows}

    all_referenced = (
        add_parent_ids + remove_parent_ids + add_child_ids + remove_child_ids
    )
    missing = [mid for mid in all_referenced if mid not in all_node_ids]
    if missing:
        raise ValueError(f"Category IDs not found: {missing}")

    bad_parent_removes = [p for p in remove_parent_ids if p not in current_parent_ids]
    if bad_parent_removes:
        raise ValueError(
            f"These are not current parents of {category_id!r}: {bad_parent_removes}"
        )
    bad_child_removes = [c for c in remove_child_ids if c not in current_child_ids]
    if bad_child_removes:
        raise ValueError(
            f"These are not current children of {category_id!r}: {bad_child_removes}"
        )

    graph = nx.DiGraph()
    graph.add_nodes_from(all_node_ids)
    for row in edge_rows:
        graph.add_edge(row[0], row[1])

    old_ancestors: set[str] = nx.ancestors(graph, category_id)
    old_descendants: set[str] = nx.descendants(graph, category_id)

    for parent_id in remove_parent_ids:
        graph.remove_edge(parent_id, category_id)
    for child_id in remove_child_ids:
        graph.remove_edge(category_id, child_id)
    for parent_id in add_parent_ids:
        graph.add_edge(parent_id, category_id)
    for child_id in add_child_ids:
        graph.add_edge(category_id, child_id)

    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError(
            "This change would introduce a cycle in the category hierarchy."
        )
    if not nx.is_weakly_connected(graph):
        raise ValueError("This change would result in a disconnected category tree.")

    ugraph = graph.to_undirected()
    new_ancestors: set[str] = nx.ancestors(graph, category_id)
    new_descendants: set[str] = nx.descendants(graph, category_id)

    new_ancestor_entries = [
        (a, category_id, nx.shortest_path_length(ugraph, a, category_id))
        for a in new_ancestors
    ]
    new_descendant_entries = [
        (category_id, d, nx.shortest_path_length(ugraph, category_id, d))
        for d in new_descendants
    ]

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
        for parent_id in remove_parent_ids:
            conn.execute(
                text(
                    "DELETE FROM public.category_edges WHERE parent_id = :parent_id AND child_id = :child_id"
                ),
                {"parent_id": parent_id, "child_id": category_id},
            )
        for child_id in remove_child_ids:
            conn.execute(
                text(
                    "DELETE FROM public.category_edges WHERE parent_id = :parent_id AND child_id = :child_id"
                ),
                {"parent_id": category_id, "child_id": child_id},
            )
        for parent_id in add_parent_ids:
            conn.execute(
                text(
                    "INSERT INTO public.category_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": parent_id, "child_id": category_id},
            )
        for child_id in add_child_ids:
            conn.execute(
                text(
                    "INSERT INTO public.category_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": category_id, "child_id": child_id},
            )

        conn.execute(
            text(
                "DELETE FROM public.category_tree WHERE ancestor_id = :cid OR descendant_id = :cid"
            ),
            {"cid": category_id},
        )

        for a in affected_ancestors:
            for d in affected_descendants:
                conn.execute(
                    text(
                        "DELETE FROM public.category_tree WHERE ancestor_id = :a AND descendant_id = :d"
                    ),
                    {"a": a, "d": d},
                )

        for ancestor_id, descendant_id, depth in (
            new_ancestor_entries + new_descendant_entries + cross_entries
        ):
            conn.execute(
                text(
                    """
                    INSERT INTO public.category_tree (ancestor_id, descendant_id, depth)
                    VALUES (:ancestor_id, :descendant_id, :depth)
                    ON CONFLICT (ancestor_id, descendant_id) DO UPDATE
                        SET depth = EXCLUDED.depth
                        WHERE public.category_tree.depth > EXCLUDED.depth
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
                INSERT INTO public.category_history (category_id, datetime, user_id, original, changes)
                VALUES (:category_id, NOW(), :user_id, :original, :changes)
                """
            ),
            {
                "category_id": category_id,
                "user_id": user_id,
                "original": original_json,
                "changes": changes_json,
            },
        )

    print(f"Updated edges for category {category_id!r}")
    wmill.run_script_by_path_async(
        "f/search/categories/index_categories", args={"keys": [category_id]}
    )
    return {"id": category_id}
