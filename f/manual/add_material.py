# requirements: project

import json
from typing import Any

import nanoid
import networkx as nx
import wmill
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine


def main(
    name: dict[str, Any],
    source: dict[str, Any],
    technical: bool,
    parent_ids: list[str],
    user_id: str,
    child_ids: list[str] | None = None,
    desc: dict[str, Any] | None = None,
    shape: str | None = None,
) -> dict[str, Any]:
    """
    Adds a new material to the DB.
    Inserts into public.materials, public.material_edges, public.material_tree,
    and public.material_history (original=NULL, changes=snapshot of all fields).
    Validates the resulting graph is a DAG and weakly connected before committing.
    """
    if child_ids is None:
        child_ids = []

    engine = create_sql_engine()

    with engine.connect() as conn:
        node_rows = conn.execute(text("SELECT id FROM public.materials")).fetchall()
        edge_rows = conn.execute(
            text(
                "SELECT ancestor_id, descendant_id FROM public.material_tree WHERE depth = 1"
            )
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}

    missing = [pid for pid in parent_ids if pid not in all_node_ids]
    if missing:
        raise ValueError(f"Parent IDs not found in materials: {missing}")
    missing_children = [cid for cid in child_ids if cid not in all_node_ids]
    if missing_children:
        raise ValueError(f"Child IDs not found in materials: {missing_children}")

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
            "Adding this material would introduce a cycle in the material hierarchy."
        )
    if not nx.is_weakly_connected(graph):
        raise ValueError(
            "Adding this material would result in a disconnected material tree."
        )

    ugraph = graph.to_undirected()
    ancestors = list(nx.ancestors(graph, new_id))
    descendants = list(nx.descendants(graph, new_id))

    # Entries where new node is the descendant
    ancestor_entries = [
        (a, new_id, nx.shortest_path_length(ugraph, a, new_id)) for a in ancestors
    ]
    # Entries where new node is the ancestor
    descendant_entries = [
        (new_id, d, nx.shortest_path_length(ugraph, new_id, d)) for d in descendants
    ]
    # Cross-product: (ancestor_of_parent, descendant_of_child) through the new node.
    # Uses the updated graph so any new shorter paths are accounted for.
    cross_entries = [
        (a, d, nx.shortest_path_length(ugraph, a, d))
        for a in ancestors
        for d in descendants
    ]

    changes_json = json.dumps(
        {
            "name": name,
            "desc": desc,
            "source": source,
            "technical": technical,
            "shape": shape,
            "parent_ids": parent_ids,
            "child_ids": child_ids,
        }
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.materials (id, created_at, updated_at, name, "desc", source, technical, shape)
                VALUES (:id, NOW(), NOW(), :name, :desc, :source, :technical, :shape)
                """
            ),
            {
                "id": new_id,
                "name": json.dumps(name),
                "desc": json.dumps(desc) if desc is not None else None,
                "source": json.dumps(source),
                "technical": technical,
                "shape": shape,
            },
        )

        for parent_id in parent_ids:
            conn.execute(
                text(
                    "INSERT INTO public.material_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": parent_id, "child_id": new_id},
            )
        for child_id in child_ids:
            conn.execute(
                text(
                    "INSERT INTO public.material_edges (parent_id, child_id) VALUES (:parent_id, :child_id)"
                ),
                {"parent_id": new_id, "child_id": child_id},
            )

        # Insert new tree entries (ancestor/descendant of new node). Since new_id is new,
        # there are no conflicts on these rows.
        for ancestor_id, descendant_id, depth in ancestor_entries + descendant_entries:
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

        # Upsert cross-product entries — only shorten existing depths, never lengthen.
        for ancestor_id, descendant_id, depth in cross_entries:
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
                VALUES (:material_id, NOW(), :user_id, NULL, :changes)
                """
            ),
            {
                "material_id": new_id,
                "user_id": user_id,
                "changes": changes_json,
            },
        )

    print(f"Created material {new_id!r}")
    wmill.run_script_by_path_async(
        "f/search/materials/index_materials", args={"keys": [new_id]}
    )
    return {"id": new_id}
