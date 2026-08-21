# requirements: project

from typing import Any

import networkx as nx
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine

SAMPLE_LIMIT = 20
TREE_INSERT_CHUNK_SIZE = 1000


def main(fix: bool = False) -> dict[str, Any]:
    """
    Verifies public.material_tree matches the closure implied by
    public.material_edges (same depth definition as
    f/sources/materials/materials_flow.py: undirected shortest-path
    distance over the whole graph).
    Reports missing/extra/wrong-depth rows. If fix=True, rewrites
    material_tree to match the recomputed closure in the same run.
    Read-only when fix=False.
    """
    engine = create_sql_engine()

    with engine.connect() as conn:
        node_rows = conn.execute(text("SELECT id FROM public.materials")).fetchall()
        edge_rows = conn.execute(
            text("SELECT parent_id, child_id FROM public.material_edges")
        ).fetchall()
        tree_rows = conn.execute(
            text("SELECT ancestor_id, descendant_id, depth FROM public.material_tree")
        ).fetchall()

    all_node_ids: set[str] = {row[0] for row in node_rows}

    graph = nx.DiGraph()
    graph.add_nodes_from(all_node_ids)
    for row in edge_rows:
        graph.add_edge(row[0], row[1])

    if not nx.is_directed_acyclic_graph(graph):
        return {"error": "material_edges graph is not a DAG; closure is undefined."}
    if not nx.is_weakly_connected(graph):
        return {
            "error": "material_edges graph is not weakly connected; closure is undefined."
        }

    ugraph = graph.to_undirected()
    correct: dict[tuple[str, str], int] = {}
    for node in graph.nodes:
        for d in nx.descendants(graph, node):
            correct[(node, d)] = nx.shortest_path_length(ugraph, node, d)

    current: dict[tuple[str, str], int] = {
        (row[0], row[1]): row[2] for row in tree_rows
    }

    missing = [
        (a, d, depth) for (a, d), depth in correct.items() if (a, d) not in current
    ]
    extra = [
        (a, d, depth) for (a, d), depth in current.items() if (a, d) not in correct
    ]
    wrong_depth = [
        (a, d, current[(a, d)], depth)
        for (a, d), depth in correct.items()
        if (a, d) in current and current[(a, d)] != depth
    ]

    result: dict[str, Any] = {
        "checked_rows": len(current),
        "expected_rows": len(correct),
        "missing_count": len(missing),
        "extra_count": len(extra),
        "wrong_depth_count": len(wrong_depth),
        "missing_sample": missing[:SAMPLE_LIMIT],
        "extra_sample": extra[:SAMPLE_LIMIT],
        "wrong_depth_sample": wrong_depth[:SAMPLE_LIMIT],
        "clean": not missing and not extra and not wrong_depth,
    }

    if fix and not result["clean"]:
        correct_entries = [(a, d, depth) for (a, d), depth in correct.items()]
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM public.material_tree"))
            for i in range(0, len(correct_entries), TREE_INSERT_CHUNK_SIZE):
                chunk = correct_entries[i : i + TREE_INSERT_CHUNK_SIZE]
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
        result["fixed"] = True
        print(
            f"Fixed material_tree: {len(missing)} missing, {len(extra)} extra, {len(wrong_depth)} wrong-depth rows repaired."
        )
    else:
        result["fixed"] = False
        if result["clean"]:
            print("material_tree matches material_edges closure.")
        else:
            print(
                f"material_tree drift: {len(missing)} missing, {len(extra)} extra, {len(wrong_depth)} wrong-depth rows."
            )

    return result
