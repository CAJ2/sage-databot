from prefect import flow
import polars as pl

from src.utils import download_cache_file
from src.utils.logging.loggers import get_logger
from src.utils.db.crdb import db_write_dataframe


@flow
def import_off():
    """
    Import the OpenFoodFacts dataset
    https://www.openfoodfacts.org/data
    Uses the Parquet export from Hugging Face
    https://huggingface.co/datasets/openfoodfacts/product-database
    The resulting table is stored in the databot schema
    and it is imported into variants, etc with other flows
    """
    log = get_logger()

    fpath = download_cache_file(
        basepath_var="openfoodfacts_basepath",
        url="https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet",
        subdir="openfoodfacts",
    )
    df = pl.scan_parquet(fpath)

    log.info(f"Columns: {df.collect_schema().names()}")
    # Filter by products that have packaging
    df = df.filter(pl.col("packagings").ne([]))
    log.info(
        f"Filtered {df.select(pl.col('code')).count().collect()} products with packaging tags"
    )

    # The code should be unique to use as an ID, remove any duplicates
    df = df.unique("code", keep="first")
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

    write_df = df.collect()
    db_write_dataframe(write_df, "off_products", id_cols=["id"])


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    import_off()
