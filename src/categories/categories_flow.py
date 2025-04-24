from prefect import flow
import polars as pl
import networkx as nx
from prefect_sqlalchemy import SqlAlchemyConnector

from src.utils.db.crdb import db_write_dataframe
from src.utils.logging.loggers import get_logger


@flow
def categories_flow():
    """
    This flow orchestrates the categories pipeline.
    The main steps are: validation of the categories graph and SQL data loading.
    """
    log = get_logger()

    categories_df = pl.read_csv(
        "src/categories/categories.tsv", separator="\t", has_header=True
    )
    name_cols = {}
    desc_short_cols = {}
    desc_cols = {}
    for col in categories_df.columns:
        if col.startswith("name:"):
            name_cols[col] = col.split(":")[1]
        elif col.startswith("desc_short:"):
            desc_short_cols[col] = col.split(":")[1]
        elif col.startswith("desc:"):
            desc_cols[col] = col.split(":")[1]
    # Compute name, desc_short, desc JSON columns from separate language columns
    categories_df = categories_df.rename(name_cols)
    categories_df = categories_df.with_columns(
        pl.struct(pl.col(name_cols.values())).struct.json_encode().alias("name")
    )
    categories_df = categories_df.drop(name_cols.values())
    categories_df = categories_df.rename(desc_short_cols)
    categories_df = categories_df.with_columns(
        pl.struct(pl.col(desc_short_cols.values()))
        .struct.json_encode()
        .alias("desc_short")
    )
    categories_df = categories_df.drop(desc_short_cols.values())
    categories_df = categories_df.rename(desc_cols)
    categories_df = categories_df.with_columns(
        pl.struct(pl.col(desc_cols.values())).struct.json_encode().alias("desc")
    )
    categories_df = categories_df.drop(desc_cols.values())
    cat_edge_df = pl.read_csv(
        "src/categories/categories_edges.tsv", separator="\t", has_header=True
    )

    # Add all nodes and edges to the graph and validate
    graph = nx.DiGraph()
    graph.add_node("CATEGORY_ROOT")

    for row in categories_df.iter_rows(named=True):
        graph.add_node(row["id"])

    for row in cat_edge_df.iter_rows(named=True):
        graph.add_edge(row["id_from"], row["id_to"])

    log.info(f"Number of nodes: {graph.number_of_nodes()}")
    log.info(f"Number of edges: {graph.number_of_edges()}")
    log.info("Running graph validation...")
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Graph is not a directed acyclic graph (DAG)")
    if not nx.is_weakly_connected(graph):
        smallest = min(nx.weakly_connected_components(graph), key=len)
        named_smallest = (
            categories_df.filter(pl.col("id").is_in(list(smallest)))
            .select("name")
            .to_series()
            .to_list()
        )
        log.error(
            f"Graph is not weakly connected. Smallest component: {named_smallest}"
        )
        raise ValueError("Graph is not weakly connected")
    log.info("Graph is a valid categories DAG")

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
    log.info(tree_df.glimpse())

    db_write_dataframe(categories_df, "categories_load")
    db_write_dataframe(
        tree_df, "categories_tree_load", id_cols=["ancestor_id", "descendant_id"]
    )
    db_write_dataframe(
        edges_df, "categories_edges_load", id_cols=["parent_id", "child_id"]
    )

    crdb = SqlAlchemyConnector.load("crdb-sage")
    crdb.execute("""
        UPSERT INTO public.categories (id, updated_at, name)
        VALUES ('CATEGORY_ROOT', NOW(), '{"xx": "Category Root"}');
    """)
    crdb.execute("""
        INSERT INTO public.categories (id, created_at, updated_at, name, "desc_short", "desc", image_url)
        SELECT id, NOW(), NOW(), name::JSONB, "desc_short"::JSONB, "desc"::JSONB, image_url::STRING
        FROM databot.categories_load
        ON CONFLICT (id) DO UPDATE
        SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
            "desc_short" = JSON_STRIP_NULLS(EXCLUDED."desc_short"::JSONB),
            "desc" = JSON_STRIP_NULLS(EXCLUDED."desc"::JSONB),
            image_url = EXCLUDED.image_url::STRING,
            updated_at = NOW();
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.categories_load;")
    crdb.execute("""
        UPSERT INTO public.category_tree (ancestor_id, descendant_id, depth)
        SELECT ancestor_id, descendant_id, depth
        FROM databot.categories_tree_load;
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.categories_tree_load;")
    crdb.execute("""
        UPSERT INTO public.category_edges (parent_id, child_id)
        SELECT parent_id, child_id
        FROM databot.categories_edges_load;
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.categories_edges_load;")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    categories_flow()
