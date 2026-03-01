# requirements: project

import polars as pl
import networkx as nx
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine, db_write_dataframe
from f.utils.git import checkout_repo


def main():
    """
    Orchestrates the materials pipeline.
    Validates the materials graph (DAG) and loads materials into CockroachDB.
    """
    checkout_repo()

    materials_df = pl.read_csv(
        "databot/src/materials/materials.tsv", separator="\t", has_header=True
    )
    name_cols = {}
    desc_cols = {}
    for col in materials_df.columns:
        if col.startswith("name:"):
            name_cols[col] = col.split(":")[1]
        elif col.startswith("desc:"):
            desc_cols[col] = col.split(":")[1]
    # Compute name and desc JSON columns from separate language columns
    materials_df = materials_df.rename(name_cols)
    materials_df = materials_df.with_columns(
        pl.struct(pl.col(name_cols.values())).struct.json_encode().alias("name")
    )
    materials_df = materials_df.drop(name_cols.values())
    materials_df = materials_df.rename(desc_cols)
    materials_df = materials_df.with_columns(
        pl.struct(pl.col(desc_cols.values())).struct.json_encode().alias("desc")
    )
    materials_df = materials_df.drop(desc_cols.values())
    mat_edge_df = pl.read_csv(
        "databot/src/materials/materials_edges.tsv", separator="\t", has_header=True
    )

    # Add all nodes and edges to the graph and validate
    graph = nx.DiGraph()
    graph.add_node("MATERIAL_ROOT")

    for row in materials_df.iter_rows(named=True):
        graph.add_node(row["id"])

    for row in mat_edge_df.iter_rows(named=True):
        graph.add_edge(row["id_from"], row["id_to"])

    print(f"Number of nodes: {graph.number_of_nodes()}")
    print(f"Number of edges: {graph.number_of_edges()}")
    print("Running graph validation...")
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Graph is not a directed acyclic graph (DAG)")
    if not nx.is_weakly_connected(graph):
        smallest = min(nx.weakly_connected_components(graph), key=len)
        named_smallest = (
            materials_df.filter(pl.col("id").is_in(list(smallest)))
            .select("name")
            .to_series()
            .to_list()
        )
        print(f"Graph is not weakly connected. Smallest component: {named_smallest}")
        raise ValueError("Graph is not weakly connected")
    print("Graph is a valid Materials DAG")

    tree_df = pl.DataFrame(
        schema={
            "ancestor_id": pl.Utf8,
            "descendant_id": pl.Utf8,
            "depth": pl.Int64,
        }
    )
    edges_df = pl.DataFrame(
        schema={
            "parent_id": pl.Utf8,
            "child_id": pl.Utf8,
        }
    )
    ugraph = graph.to_undirected()
    for node in graph.nodes:
        descendants = list(nx.descendants(graph, node))
        d_depth = (
            nx.shortest_path_length(ugraph, source=node, target=d) for d in descendants
        )
        tree_df = tree_df.vstack(
            pl.DataFrame(
                {
                    "ancestor_id": node,
                    "descendant_id": descendants,
                    "depth": d_depth,
                }
            )
        )
        edges_df = edges_df.vstack(
            pl.DataFrame(
                {
                    "parent_id": node,
                    "child_id": list(nx.descendants_at_distance(graph, node, 1)),
                }
            )
        )
    print(tree_df.glimpse())

    db_write_dataframe(materials_df, "materials_load")
    db_write_dataframe(
        tree_df, "materials_tree_load", id_cols=["ancestor_id", "descendant_id"]
    )
    db_write_dataframe(
        edges_df, "materials_edges_load", id_cols=["parent_id", "child_id"]
    )

    engine = create_sql_engine()
    with engine.begin() as crdb:
        crdb.execute(
            text("""
            UPSERT INTO public.materials (id, updated_at, name, source, technical)
            VALUES ('MATERIAL_ROOT', NOW(), '{"xx": "Material Root"}', '{}', FALSE);
        """)
        )
        crdb.execute(
            text("""
            INSERT INTO public.materials (id, created_at, updated_at, name, "desc", source, technical, shape)
            SELECT id, NOW(), NOW(), name::JSONB, "desc"::JSONB, source::JSONB, technical::BOOLEAN, shape
            FROM databot.materials_load
            ON CONFLICT (id) DO UPDATE
            SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                "desc" = JSON_STRIP_NULLS(EXCLUDED."desc"::JSONB),
                source = EXCLUDED.source::JSONB,
                technical = EXCLUDED.technical::BOOLEAN,
                shape = EXCLUDED.shape,
                updated_at = NOW();
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.materials_load;"))
        crdb.execute(
            text("""
            UPSERT INTO public.material_tree (ancestor_id, descendant_id, depth)
            SELECT ancestor_id, descendant_id, depth
            FROM databot.materials_tree_load;
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.materials_tree_load;"))
        crdb.execute(
            text("""
            UPSERT INTO public.material_edges (parent_id, child_id)
            SELECT parent_id, child_id
            FROM databot.materials_edges_load;
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.materials_edges_load;"))
