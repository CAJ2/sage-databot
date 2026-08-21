# requirements: project

import json
from typing import Any

import nanoid
import networkx as nx
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine

TREE_INSERT_CHUNK_SIZE = 1000


def main(
    name: dict[str, Any],
    parent_ids: list[str],
    user_id: str,
    child_ids: list[str] | None = None,
    desc_short: dict[str, Any] | None = None,
    desc: dict[str, Any] | None = None,
    image_url: str | None = None,
) -> dict[str, Any]:
    """
    Adds a new category to the DB.
    Inserts into public.categories, public.category_edges, public.category_tree,
    and public.category_history (original=NULL, changes=snapshot of all fields).
    Validates the resulting graph is a DAG and weakly connected before committing.
    """
    if child_ids is None:
        child_ids = []

    engine = create_sql_engine()

    with engine.connect() as conn:
        node_rows = conn.execute(text("SELECT id FROM public.categories")).fetchall()
        edge_rows = conn.execute(
            text(
                "SELECT ancestor_id, descendant_id FROM public.category_tree WHERE depth = 1"
            )
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}

    missing = [pid for pid in parent_ids if pid not in all_node_ids]
    if missing:
        raise ValueError(f"Parent IDs not found in categories: {missing}")
    missing_children = [cid for cid in child_ids if cid not in all_node_ids]
    if missing_children:
        raise ValueError(f"Child IDs not found in categories: {missing_children}")

    new_id = nanoid.generate()

    graph = nx.DiGraph()
    graph.add_nodes_from(all_node_ids)
    graph.add_node(new_id)
    for row in edge_rows:
        graph.add_edge(row[0], row[1])
    for parent_id in parent_ids:
        graph.add_edge(parent_id, new_id)
    for child_id in child_ids:
        graph.add_edge(new_id, child_id)

    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError(
            "Adding this category would introduce a cycle in the category hierarchy."
        )
    if not nx.is_weakly_connected(graph):
        raise ValueError(
            "Adding this category would result in a disconnected category tree."
        )

    # Recompute the full closure over the whole graph. Adding a node/edges
    # can create an undirected shortcut that alters shortest-path depths
    # between unrelated nodes elsewhere in the DAG, so depth cannot be
    # bounded to the new node's own ancestor/descendant cross-product.
    ugraph = graph.to_undirected()
    tree_entries: list[tuple[str, str, int]] = []
    for node in graph.nodes:
        for d in nx.descendants(graph, node):
            tree_entries.append((node, d, nx.shortest_path_length(ugraph, node, d)))

    changes_json = json.dumps(
        {
            "name": name,
            "desc_short": desc_short,
            "desc": desc,
            "image_url": image_url,
            "parent_ids": parent_ids,
            "child_ids": child_ids,
        }
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.categories (id, created_at, updated_at, name, desc_short, "desc", image_url)
                VALUES (:id, NOW(), NOW(), :name, :desc_short, :desc, :image_url)
                """
            ),
            {
                "id": new_id,
                "name": json.dumps(name),
                "desc_short": json.dumps(desc_short)
                if desc_short is not None
                else None,
                "desc": json.dumps(desc) if desc is not None else None,
                "image_url": image_url,
            },
        )

        for parent_id in parent_ids:
            conn.execute(
                text(
                    "INSERT INTO public.category_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": parent_id, "child_id": new_id},
            )
        for child_id in child_ids:
            conn.execute(
                text(
                    "INSERT INTO public.category_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": new_id, "child_id": child_id},
            )

        # Full rebuild: clear and reinsert the whole closure so depths stay
        # correct even where the change had ripple effects far from new_id.
        # Inserted in chunks of multi-row VALUES to keep this to a handful of
        # round trips instead of one per row.
        conn.execute(text("DELETE FROM public.category_tree"))
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
                    f"INSERT INTO public.category_tree (ancestor_id, descendant_id, depth) VALUES {placeholders}"
                ),
                params,
            )

        conn.execute(
            text(
                """
                INSERT INTO public.category_history (category_id, datetime, user_id, original, changes)
                VALUES (:category_id, NOW(), :user_id, NULL, :changes)
                """
            ),
            {
                "category_id": new_id,
                "user_id": user_id,
                "changes": changes_json,
            },
        )

    print(f"Created category {new_id!r}")
    wmill.run_script_by_path_async(
        "f/search/categories/index_categories", args={"keys": [new_id]}
    )
    return {"id": new_id}
