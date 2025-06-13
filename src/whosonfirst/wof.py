import os
from prefect import flow, task
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
import polars as pl
import bz2
import json

from src.utils.logging.loggers import get_logger
from src.utils.db.crdb import create_polars_uri
from src.cli import setup_cli

placetype_admin = [
    ["continent", "1"],
    ["empire", "1"],
    ["dependency", "1"],
    ["disputed", "1"],
    ["macroregion", "1"],
    ["marketarea", "1"],
    ["country", "2"],
    ["macrocounty", "4"],
    ["macrohood", "4"],
    ["microhood", "4"],
    ["postalregion", "4"],
    ["region", "4"],
    ["county", "6"],
    ["localadmin", "8"],
    ["neighbourhood", "10"],
    ["postalcode", "11"],
    ["locality", "11"],
]


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
        query="SELECT id, placetype, language, script, region, name FROM names WHERE privateuse = 'preferred' "
        + f"AND placetype IN ({', '.join([f"'{i[0]}'" for i in placetype_admin])})",
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

    allowed_placetypes = [i[0] for i in placetype_admin]
    replace_with = [i[1] for i in placetype_admin]
    lang_df = lang_df.with_columns(
        pl.col("placetype")
        .str.replace_many(allowed_placetypes, replace_with)
        .str.to_integer(strict=False)
        .alias("admin_level")
    )

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

    def filter_props(props: str) -> dict | None:
        """
        Filter out records that do not have proper hierarchies or placetypes.
        Also create a hierarchy with an admin_level ordering.
        """
        props_json = json.loads(props)
        if "wof:hierarchy" not in props_json or "wof:placetype" not in props_json:
            return None
        if props_json["wof:placetype"] not in allowed_placetypes:
            return None
        hierarchy = props_json["wof:hierarchy"]
        if not isinstance(hierarchy, list) or len(hierarchy) < 1:
            return None
        if len(hierarchy[0].keys()) <= 1:
            return None
        new_props = {}
        for k, v in props_json.items():
            if not (k.startswith("name:") or k.startswith("ne:")):
                new_props[k] = v
        # Match hierarchy ids to the df and join admin_level
        new_props["hierarchy"] = []
        for h in hierarchy[0].values():
            h_lookup = lang_df.filter(pl.col("id") == h).select(
                "id", "placetype", "admin_level"
            )
            if h_lookup.is_empty():
                continue
            h_lookup = h_lookup.to_dicts()[0]
            if h_lookup["admin_level"]:
                new_props["hierarchy"].append(
                    {
                        "id": h_lookup["id"],
                        "placetype": h_lookup["placetype"],
                        "admin_level": h_lookup["admin_level"],
                    }
                )
        # If the hierarchy doesn't look reasonable, skip it
        if len(new_props["hierarchy"]) < 2 and props_json["wof:placetype"] != "country":
            return None
        new_props["hierarchy"] = sorted(
            new_props["hierarchy"], key=lambda x: x["admin_level"], reverse=True
        )

        return json.dumps(new_props)

    geojson_df = geojson_df.with_columns(
        pl.col("properties")
        .map_elements(filter_props, return_dtype=pl.String)
        .alias("properties")
    )
    geojson_df = geojson_df.filter(pl.col("properties").is_not_null())
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
        INSERT INTO public.regions (id, created_at, updated_at, name, geo, properties, placetype, admin_level)
        SELECT 'wof_' || id, NOW(), NOW(), JSON_STRIP_NULLS(name::JSONB),
            ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_GEOMFROMGEOJSON(geo::JSONB)))),
            properties::JSONB, placetype, admin_level
        FROM databot.regions_wof_load
        ON CONFLICT (id) DO UPDATE
        SET placetype = EXCLUDED.placetype,
            name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
            geo = ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_GEOMFROMGEOJSON(EXCLUDED.geo::JSONB)))),
            properties = EXCLUDED.properties::JSONB,
            admin_level = EXCLUDED.admin_level,
            updated_at = NOW();
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.regions_wof_load;")
    crdb.execute(
        "UPDATE regions SET \"name\" = jsonb_set(\"name\", '{xx}', properties->'wof:name');"
    )


@flow
def import_whosonfirst_admin(country: list[str], **kwargs):
    """
    This flow imports the Whos On First administrative data from Geocode Earth.
    The data is then transformed and loaded into the main database.
    The admin data is used to populate the regions table.

    https://github.com/whosonfirst-data/whosonfirst-data/
    https://geocode.earth/data/whosonfirst/combined/
    """
    log = get_logger()

    if len(country) == 0:
        raise ValueError("No country provided. Please specify a country to import.")
    country = country[0]
    log.info(f"Importing region data for {country.upper()}...")
    download_url = f"https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-{country.lower()}-latest.db.bz2"
    filepath = load_whosonfirst(country, download_url)
    transform_whosonfirst(filepath)


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "country",
            type=str,
            nargs="+",
            help="The country to process.",
        )

    setup_cli(import_whosonfirst_admin, args)
