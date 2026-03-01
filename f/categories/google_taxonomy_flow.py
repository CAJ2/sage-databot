# requirements: project

import polars as pl
import networkx as nx
import nanoid
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine, db_write_dataframe


def get_product_df_from_language_code(code: str):
    print(f"Loading Google product taxonomy for {code}...")
    df_columns = "id cat_1 cat_2 cat_3 cat_4 cat_5 cat_6 cat_7".split()
    coalesce_cols = df_columns[1:]
    coalesce_cols.reverse()
    try:
        df: pl.DataFrame = pl.read_excel(
            f"data/taxonomy/taxonomy-with-ids.{code}.xls",
            has_header=False,
            infer_schema_length=None,
        )
    except Exception:
        try:
            df: pl.DataFrame = pl.read_csv(
                f"https://www.google.com/basepages/producttype/taxonomy-with-ids.{code}.txt",
                has_header=False,
                infer_schema=True,
                infer_schema_length=None,
            )
        except Exception:
            return ()
        df = df.with_columns(
            pl.col("column_1")
            .str.split_exact(" - ", 1)
            .struct.rename_fields(["id", "cat"])
        ).unnest("column_1")
        df = df.with_columns(
            pl.col("cat")
            .str.split_exact(" > ", 7)
            .struct.rename_fields(
                ["cat_1", "cat_2", "cat_3", "cat_4", "cat_5", "cat_6", "cat_7"]
            )
        ).unnest("cat")
        df = df.drop(
            "column_2",
            "column_3",
            "column_4",
            "column_5",
            "column_6",
            "column_7",
            strict=False,
        )
    df = df.rename(dict(zip(df.columns, df_columns)))
    df = df.with_columns(pl.col("id").cast(pl.Utf8))
    print(f"Loaded {df.shape[0]} rows for {code}")
    print(f"Columns: {df.columns}")
    df = df.vstack(
        pl.DataFrame(
            {
                "id": ["CATEGORY_ROOT"],
                "cat_1": [None],
                "cat_2": [None],
                "cat_3": [None],
                "cat_4": [None],
                "cat_5": [None],
                "cat_6": [None],
                "cat_7": [None],
            }
        )
    )
    print(f"Loaded {df.shape[0]} rows for {code}")

    name_col = f"name:{code}"
    df = df.with_columns(pl.coalesce(*coalesce_cols).alias(name_col))

    df_edges = pl.DataFrame(
        schema={
            "id_from": pl.Utf8,
            "id_to": pl.Utf8,
        }
    )
    to_remove = ["Mature", "Tobacco"]
    df = df.filter(
        ~(pl.concat_str(coalesce_cols, ignore_nulls=True).str.contains_any(to_remove))
    )
    print(f"After filter: {df.shape[0]} categories for {code}")
    if code != "en-US":
        df = df.drop(coalesce_cols)
        df = df.drop("column_2", "column_3", "column_4", "column_5", strict=False)
        return (df, None)
    # Find the first and second non-null names and create an edge between them
    for row in df.iter_rows(named=True):
        id_to = row["id"]
        if id_to == "CATEGORY_ROOT":
            continue
        name_to = row[name_col]
        if not name_to:
            continue
        found_name = False
        id_from = None
        # Find the first non-null name in the next columns
        for i in range(0, len(coalesce_cols)):
            name_from = row[coalesce_cols[i]]
            if name_from is None:
                continue
            if not found_name:
                found_name = True
                continue
            if found_name:
                id_from = (
                    df.filter(pl.col(name_col).eq(name_from))
                    .select("id")
                    .to_series()
                    .to_list()
                )
                df_edges = df_edges.vstack(
                    pl.DataFrame({"id_from": id_from[0], "id_to": id_to})
                )
                found_name = False
                break
        if found_name:
            # If no second name was found, create an edge to the root category
            df_edges = df_edges.vstack(
                pl.DataFrame({"id_from": "CATEGORY_ROOT", "id_to": id_to})
            )

    df = df.drop(coalesce_cols)
    df = df.drop("column_2", "column_3", "column_4", "column_5", strict=False)

    return (df, df_edges)


def categories_flow():
    """
    This flow imports the Google product taxonomy.
    The main steps are: validation of the categories graph and SQL data loading.
    """

    code_list = [
        "en-US",
        "da-DK",
        "pt-BR",
        "tr-TR",
        "it-IT",
        "de-DE",
        "en-GB",
        "en-AU",
        "es-ES",
        "ja-JP",
        "fr-CH",
        "it-CH",
        "de-CH",
        "fr-FR",
        "pl-PL",
        "nl-NL",
        "cs-CZ",
        "ru-RU",
        "sv-SE",
        "no-NO",
    ]
    # code_list = [
    #     "en-US",
    #     "da-DK",
    #     "sv-SE",
    # ]
    categories_df = None
    cat_edge_df = None
    for code in code_list:
        dfs = get_product_df_from_language_code(code)
        if len(dfs) == 0:
            continue
        if categories_df is None:
            categories_df, cat_edge_df = dfs
        else:
            new_categories_df, new_cat_edge_df = dfs
            categories_df = categories_df.join(
                new_categories_df, on="id", how="inner", suffix=code
            )

    if categories_df is None or cat_edge_df is None:
        raise ValueError("No category data found for any language code")

    root_row = categories_df.filter(pl.col("id").eq("CATEGORY_ROOT")).to_dicts()[0]
    root_row["google_id"] = "CATEGORY_ROOT"
    categories_df = categories_df.rename({"id": "google_id"})
    categories_df = categories_df.filter(pl.col("google_id").ne("CATEGORY_ROOT"))
    new_ids = (
        categories_df.get_column("google_id")
        .map_elements(lambda x: nanoid.generate(), return_dtype=pl.Utf8)
        .sort()
        .alias("id")
    )
    categories_df = categories_df.with_columns(new_ids)
    print(root_row)
    categories_df = categories_df.vstack(
        pl.DataFrame(root_row, schema=categories_df.schema)
    )
    cat_edge_df = cat_edge_df.join(
        categories_df.select("google_id", "id").rename({"google_id": "id_from"}),
        on="id_from",
        how="inner",
    )
    cat_edge_df = cat_edge_df.drop("id_from").rename({"id": "id_from"})
    cat_edge_df = cat_edge_df.join(
        categories_df.select("google_id", "id").rename({"google_id": "id_to"}),
        on="id_to",
        how="inner",
    )
    cat_edge_df = cat_edge_df.drop("id_to").rename({"id": "id_to"})
    categories_df = categories_df.drop("google_id")
    print(f"Categories: {categories_df.head(20)}")
    print(f"Category edges: {cat_edge_df.head(20)}")
    categories_df.write_csv("data/taxonomy/categories.csv")
    cat_edge_df.write_csv("data/taxonomy/category_edges.csv")
    name_cols = {}
    for col in categories_df.columns:
        if col.startswith("name:"):
            name_cols[col] = col.split(":")[1]
    # Compute name, desc_short, desc JSON columns from separate language columns
    categories_df = categories_df.rename(name_cols)
    categories_df = categories_df.with_columns(
        pl.struct(pl.col(name_cols.values())).struct.json_encode().alias("name")
    )
    categories_df = categories_df.drop(name_cols.values())

    # Add all nodes and edges to the graph and validate
    graph = nx.DiGraph()
    graph.add_node("CATEGORY_ROOT")

    for row in categories_df.iter_rows(named=True):
        graph.add_node(row["id"])

    for row in cat_edge_df.iter_rows(named=True):
        graph.add_edge(row["id_from"], row["id_to"])

    print(f"Number of nodes: {graph.number_of_nodes()}")
    print(f"Number of edges: {graph.number_of_edges()}")
    print("Running graph validation...")
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
        print(f"Graph is not weakly connected. Smallest component: {named_smallest}")
        raise ValueError("Graph is not weakly connected")
    print("Graph is a valid categories DAG")

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

    db_write_dataframe(categories_df, "categories_load")
    db_write_dataframe(
        tree_df, "categories_tree_load", id_cols=["ancestor_id", "descendant_id"]
    )
    db_write_dataframe(
        edges_df, "categories_edges_load", id_cols=["parent_id", "child_id"]
    )

    engine = create_sql_engine()
    with engine.begin() as crdb:
        crdb.execute(
            text("""
            UPSERT INTO public.categories (id, updated_at, name)
            VALUES ('CATEGORY_ROOT', NOW(), '{"xx": "Category Root"}');
        """)
        )
        crdb.execute(
            text("""
            INSERT INTO public.categories (id, created_at, updated_at, name)
            SELECT id, NOW(), NOW(), name::JSONB
            FROM databot.categories_load
            ON CONFLICT (id) DO UPDATE
            SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                updated_at = NOW();
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.categories_load;"))
        crdb.execute(
            text("""
            UPSERT INTO public.category_tree (ancestor_id, descendant_id, depth)
            SELECT ancestor_id, descendant_id, depth
            FROM databot.categories_tree_load;
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.categories_tree_load;"))
        crdb.execute(
            text("""
            UPSERT INTO public.category_edges (parent_id, child_id)
            SELECT parent_id, child_id
            FROM databot.categories_edges_load;
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.categories_edges_load;"))


def main():
    categories_flow()
