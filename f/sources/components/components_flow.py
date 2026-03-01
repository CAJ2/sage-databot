# requirements: project

import polars as pl
from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine, db_write_dataframe
from f.utils.db.meili import meili_connect
from f.utils.git import checkout_repo


def main():
    """
    Orchestrates the components pipeline.
    Reads components from the TSV file, resolves material IDs via Meilisearch,
    and loads the data into CockroachDB.
    """
    checkout_repo()

    meili = meili_connect()

    comp_df = pl.read_csv(
        "databot/src/components/components.tsv", separator="\t", has_header=True
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
    comp_df = comp_df.drop_nulls("primary_material_id").with_columns(
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
    print(f"Df: {comp_mat_df.drop_nulls('materials').head(50)}")
    comp_mat_df = (
        comp_mat_df.drop_nulls("materials")
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
    comp_mat_df = comp_mat_df.rename({"map": "material_id"}).drop_nulls("material_id")
    comp_mat_df = comp_mat_df.select(
        pl.col("component_id", "material_id", "material_fraction")
    ).drop_nulls("material_id")

    db_write_dataframe(comp_df, "components_load")
    db_write_dataframe(
        comp_mat_df,
        "components_materials_load",
        id_cols=["component_id", "material_id"],
    )

    engine = create_sql_engine()
    with engine.begin() as crdb:
        crdb.execute(
            text("""
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
        )
        crdb.execute(
            text("""
            UPSERT INTO public.components_materials (component_id, material_id, material_fraction)
            SELECT component_id, material_id, material_fraction
            FROM databot.components_materials_load;
        """)
        )
        crdb.execute(text("DROP TABLE IF EXISTS databot.components_load;"))
        crdb.execute(text("DROP TABLE IF EXISTS databot.components_materials_load;"))
