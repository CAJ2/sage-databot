from prefect import flow
import polars as pl
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
import meilisearch

from src.cli import setup_cli
from src.utils.db.crdb import db_write_dataframe
from src.utils.logging.loggers import get_logger


@flow
def components_flow(**kwargs):
    """
    This flow orchestrates the components pipeline.
    """
    log = get_logger()

    # Connect to Meilisearch
    meili = meilisearch.Client(
        Variable.get("meilisearch", default="http://localhost:7700"),
    )

    comp_df = pl.read_csv(
        "src/components/components.tsv", separator="\t", has_header=True
    )
    name_cols = {}
    desc_cols = {}
    for col in comp_df.columns:
        if col.startswith("name:"):
            name_cols[col] = col.split(":")[1]
        elif col.startswith("desc:"):
            desc_cols[col] = col.split(":")[1]
    # Compute name and desc JSON columns from separate language columns
    comp_df = comp_df.rename(name_cols)
    comp_df = comp_df.with_columns(
        pl.struct(pl.col(name_cols.values())).struct.json_encode().alias("name")
    )
    comp_df = comp_df.drop(name_cols.values())
    comp_df = comp_df.rename(desc_cols)
    comp_df = comp_df.with_columns(
        pl.struct(pl.col(desc_cols.values())).struct.json_encode().alias("desc")
    )
    comp_df = comp_df.drop(desc_cols.values())

    def get_primary_mat(row):
        mat = meili.index("materials").search(row, {"limit": 1})
        if len(mat["hits"]) == 0:
            return None
        else:
            return mat["hits"][0]["id"]

    comp_df = comp_df.with_columns(
        pl.col("primary_material:en")
        .map_elements(get_primary_mat)
        .alias("primary_material_id")
    )
    comp_df = comp_df.drop_nulls(pl.col("primary_material_id")).with_columns(
        pl.lit(None).cast(pl.String).alias("region_id"),
        pl.lit(None).cast(pl.String).alias("visual"),
    )
    comp_mat_df = comp_df.with_columns(
        pl.col("id").alias("component_id"),
        pl.col("materials:en").str.split(",").alias("materials"),
        pl.col("material_fraction").str.split(","),
    )
    comp_mat_df = comp_mat_df.with_columns(
        pl.when(pl.col("material_fraction").is_null())
        .then(pl.lit([1.0]))
        .otherwise(pl.col("material_fraction"))
        .alias("material_fraction"),
    )
    log.info(f"Df: {comp_mat_df.drop_nulls(pl.col('materials')).head(50)}")
    comp_mat_df = (
        comp_mat_df.drop_nulls(pl.col("materials"))
        .explode(["materials", "material_fraction"])
        .with_columns(pl.col("material_fraction").cast(pl.Float32))
    )

    def get_mat(row):
        mat = meili.index("materials").search(row[0], {"limit": 1})
        if len(mat["hits"]) == 0:
            return None
        else:
            return mat["hits"][0]["id"]

    comp_mat_df = comp_mat_df.with_columns(
        comp_mat_df.select(pl.col("materials")).map_rows(get_mat)
    )
    comp_mat_df = comp_mat_df.rename({"map": "material_id"}).drop_nulls(
        pl.col("material_id")
    )
    comp_mat_df = comp_mat_df.select(
        pl.col("component_id", "material_id", "material_fraction")
    ).drop_nulls(pl.col("material_id"))

    db_write_dataframe(comp_df, "components_load")
    db_write_dataframe(
        comp_mat_df,
        "components_materials_load",
        id_cols=["component_id", "material_id"],
    )

    crdb = SqlAlchemyConnector.load("crdb-sage")
    crdb.execute("""
        INSERT INTO public.components (id, created_at, updated_at, name, "desc", region_id, primary_material_id, visual)
        SELECT id, NOW(), NOW(), name::JSONB, "desc"::JSONB, region_id, primary_material_id, visual::JSONB
        FROM databot.components_load
        ON CONFLICT (id) DO UPDATE
        SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
            "desc" = JSON_STRIP_NULLS(EXCLUDED."desc"::JSONB),
            region_id = EXCLUDED.region_id,
            primary_material_id = EXCLUDED.primary_material_id,
            visual = JSON_STRIP_NULLS(EXCLUDED.visual::JSONB),
            updated_at = NOW();
    """)
    crdb.execute("""
        UPSERT INTO public.components_materials (component_id, material_id, material_fraction)
        SELECT component_id, material_id, material_fraction
        FROM databot.components_materials_load;
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.components_load;")
    crdb.execute("DROP TABLE IF EXISTS databot.components_materials_load;")


if __name__ == "__main__":
    setup_cli(components_flow)
