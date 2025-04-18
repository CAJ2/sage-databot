import os
from prefect import flow, task
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
import polars as pl
from src.utils.logging.loggers import get_logger
from src.utils.db.crdb import create_polars_uri
import bz2


@task
def load_whosonfirst(country: str, download_url: str):
    """
    Download the Whos On First country data from the given URL.
    """
    log = get_logger()
    # Download the file
    filename = os.path.basename(download_url)
    basepath = Variable.get("whosonfirst_basepath")
    if not basepath:
        basepath = os.path.join(os.getcwd(), "data", "whosonfirst")
        Variable.put("whosonfirst_basepath", basepath)
    if not os.path.isabs(basepath):
        basepath = os.path.join(os.getcwd(), basepath)
    filepath = os.path.join(basepath, country, filename)

    if not os.path.exists(filepath):
        log.info(f"Downloading {download_url} to {filepath}")
        os.system(f"curl -o {filepath} {download_url}")
    else:
        log.info(f"File {filepath} already exists, skipping download.")

    # Extract the bzip2 file using bz2 python module
    extractpath = filepath[:-4]
    if os.path.exists(extractpath):
        log.info(f"File {extractpath} already exists, skipping extraction.")
        return extractpath
    log.info(f"Extracting {filepath}")
    with bz2.BZ2File(filepath, "rb") as f_in:
        with open(extractpath, "wb") as f_out:
            f_out.write(f_in.read())
    log.info(f"Extracted {filepath} to {extractpath}")

    return extractpath


@task
def transform_whosonfirst(filepath: str):
    """
    Transform the Whos On First data.
    """
    log = get_logger()

    names_df = pl.read_database_uri(
        query="SELECT id, placetype, language, script, region, name FROM names WHERE privateuse = 'preferred'",
        uri=f"sqlite:///{filepath}",
        engine="connectorx",
    )

    names_df = names_df.with_columns(
        pl.concat_str(
            [pl.col("language"), pl.col("script"), pl.col("region")],
            separator="-",
            ignore_nulls=True,
        ).alias("lang")
    )
    lang_df = names_df.pivot(
        index=["id", "placetype"],
        on=["lang"],
        values=["name"],
        aggregate_function="first",
    )
    lang_df = lang_df.with_columns(
        pl.struct(pl.all().exclude(["id", "placetype"])).alias("name")
    )
    lang_df = lang_df.drop(pl.all().exclude(["id", "placetype", "name"]))
    lang_df = lang_df.with_columns(pl.col("name").struct.json_encode())

    geojson_df = pl.read_database_uri(
        query="SELECT id, body FROM geojson WHERE is_alt = 0",
        uri=f"sqlite:///{filepath}",
        engine="connectorx",
    )
    geojson_df = geojson_df.with_columns(
        pl.col("body").str.json_path_match("$.geometry").alias("geo"),
        pl.col("body").str.json_path_match("$.properties").alias("properties"),
    )
    geojson_df = geojson_df.drop("body")
    combined_df = lang_df.join(geojson_df, on="id", how="inner")
    combined_df = combined_df.drop_nulls(subset=["id"])
    log.info(f"Total records: {combined_df.height}")

    crdb = SqlAlchemyConnector.load("crdb-sage")
    conn = create_polars_uri(crdb)

    combined_df.write_database(
        connection=conn,
        table_name="databot.regions_wof_load",
        if_table_exists="replace",
        engine="adbc",
    )

    crdb.execute("ALTER TABLE databot.regions_wof_load ALTER COLUMN id SET NOT NULL;")
    crdb.execute(
        "ALTER TABLE databot.regions_wof_load ALTER PRIMARY KEY USING COLUMNS (id);"
    )
    crdb.execute("""
        INSERT INTO public.regions (id, created_at, updated_at, name, geo, properties, placetype)
        SELECT 'wof_' || id, NOW(), NOW(), JSON_STRIP_NULLS(name::JSONB),
            ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_GEOMFROMGEOJSON(geo::JSONB)))),
            properties::JSONB, placetype
        FROM databot.regions_wof_load
        ON CONFLICT (id) DO UPDATE
        SET placetype = EXCLUDED.placetype,
            name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
            geo = ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_GEOMFROMGEOJSON(EXCLUDED.geo::JSONB)))),
            properties = EXCLUDED.properties::JSONB,
            updated_at = NOW();
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.regions_wof_load;")


@flow
def import_whosonfirst_admin():
    """
    This flow imports the Whos On First administrative data from Geocode Earth.
    The data is then transformed and loaded into the main database.
    The admin data is used to populate the regions table.

    https://github.com/whosonfirst-data/whosonfirst-data/
    https://geocode.earth/data/whosonfirst/combined/
    """
    log = get_logger()

    countries: list[str] = Variable.get("whosonfirst_countries")
    if not countries:
        log.error("No countries found in the whosonfirst_countries variable.")
        return

    for country in countries:
        log.info(f"Importing region data for {country.upper()}...")
        download_url = f"https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-{country.lower()}-latest.db.bz2"
        filepath = load_whosonfirst(country, download_url)
        transform_whosonfirst(filepath)


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    import_whosonfirst_admin()
