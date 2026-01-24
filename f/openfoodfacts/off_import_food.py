# requirements: project

from wmill import S3Object
import polars as pl

from f.utils.db.crdb import db_write_dataframe
from f.utils.s3 import S3Client


def import_off(parquet_file: S3Object):
    """
    Import the OpenFoodFacts dataset
    https://www.openfoodfacts.org/data
    Uses the Parquet export from Hugging Face
    https://huggingface.co/datasets/openfoodfacts/product-database
    The resulting table is stored in the databot schema
    and it is imported into variants, etc with other flows
    """

    s3 = S3Client()
    url = s3.create_url(parquet_file["s3"])
    print(f"Loading Parquet file from {url}...")
    df = pl.scan_parquet(url, storage_options=s3.polars_options())

    print(f"Columns: {df.collect_schema().names()}")
    print(f"Found {df.select(pl.col('code')).count().collect()} products")

    # The code should be unique to use as an ID, remove any duplicates
    # Cannot run for performance reasons, so we do it with duckdb in a separate step
    # df = df.unique("code", keep="any")
    # Select only the columns we need
    df = df.select(
        pl.col(
            "code",
            "brands",
            "categories",
            "cities_tags",
            "countries_tags",
            "data_sources_tags",
            "ecoscore_data",
            "emb_codes",
            "generic_name",
            "images",
            "labels",
            "lang",
            "link",
            "manufacturing_places",
            "origins",
            "packagings",
            "product_name",
            "product_quantity",
            "product_quantity_unit",
            "stores",
        )
    )
    df = df.with_columns(pl.concat_str([pl.lit("off_"), pl.col("code")]).alias("id"))
    df = df.drop("code")

    db_write_dataframe(df, "off_products", id_cols=["id"], chunk_size=500)


def main(parquet_file: S3Object):
    import_off(parquet_file)
