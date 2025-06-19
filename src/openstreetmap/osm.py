from prefect import flow, task
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
import polars as pl
import osmium as osm
import json

from src.openstreetmap.generators import generate_name, generate_address
from src.openstreetmap.osm_tags import waste_tags
from src.utils import download_cache_file
from src.utils.logging.loggers import get_logger
from src.utils.db.crdb import create_polars_uri
from src.cli import setup_cli


@task
def load_osm(country: str, download_url: str):
    """
    Download the OSM country data from the given URL.
    """
    log = get_logger()
    filepath = download_cache_file("osm_basepath", download_url)
    log.info(f"Downloaded country {country} to {filepath}")

    return filepath


def construct_osm_json(o):
    """
    Construct the OSM json.
    """
    tags = {}
    for t in o.tags:
        tags[t.k] = t.v
    json_obj = {
        "timestamp": o.timestamp.isoformat(),
        "tags": tags,
    }
    return json.dumps(json_obj, ensure_ascii=False)


@task
def transform_osm(filepath: str):
    """
    Transform the OSM data.
    """
    log = get_logger()

    places_df = pl.DataFrame(
        schema={
            "id": pl.Utf8,
            "name": pl.Utf8,
            "address": pl.Utf8,
            "location": pl.Utf8,
            "osm": pl.Utf8,
        }
    )

    pbf = (
        osm.FileProcessor(filepath)
        .with_locations()
        .with_filter(osm.filter.TagFilter(*waste_tags))
    )
    rows = []
    total = 0
    for o in pbf:
        if o.is_node():
            node = (
                f"node_{o.id}",
                json.dumps(generate_name(o.tags), ensure_ascii=False),
                json.dumps(generate_address(o.tags), ensure_ascii=False),
                f"SRID=4326;POINT({o.location.lon} {o.location.lat})",
                construct_osm_json(o),
            )
            rows.append(node)
        if o.is_way():
            if len(o.nodes) == 0:
                continue
            lons = []
            lats = []
            for n in o.nodes:
                lons.append(n.lon)
                lats.append(n.lat)
            loc = None
            if len(lons) > 0:
                loc = (
                    f"SRID=4326;POINT({sum(lons) / len(lons)} {sum(lats) / len(lats)})"
                )
            way = (
                f"way_{o.id}",
                json.dumps(generate_name(o.tags), ensure_ascii=False),
                json.dumps(generate_address(o.tags), ensure_ascii=False),
                loc,
                construct_osm_json(o),
            )
            rows.append(way)
        if o.is_relation():
            # TODO: Should we handle relations?
            continue
        if len(rows) % 1000 == 0:
            total += len(rows)
            log.info(f"Processed {total} rows...")
            places_df = places_df.vstack(pl.DataFrame(rows, schema=places_df.schema))
            rows = []
    if len(rows) > 0:
        total += len(rows)
        log.info(f"Processed {total} rows...")
        places_df = places_df.vstack(
            pl.DataFrame(rows, schema=places_df.schema, orient="row")
        )
        rows = []

    crdb = SqlAlchemyConnector.load("crdb-sage")
    conn = create_polars_uri(crdb)

    places_df.write_database(
        connection=conn,
        table_name="databot.places_osm_load",
        if_table_exists="replace",
        engine="adbc",
    )

    crdb.execute("ALTER TABLE databot.places_osm_load ALTER COLUMN id SET NOT NULL;")
    crdb.execute(
        "ALTER TABLE databot.places_osm_load ALTER PRIMARY KEY USING COLUMNS (id);"
    )
    crdb.execute("""
        INSERT INTO public.places (id, created_at, updated_at, name, address, location, osm)
        SELECT id, NOW(), NOW(), name::JSONB, address::JSONB, ST_GEOGFROMEWKT(location::TEXT), osm::JSONB
        FROM databot.places_osm_load
        ON CONFLICT (id) DO UPDATE
        SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
            address = JSON_STRIP_NULLS(EXCLUDED.address::JSONB),
            location = ST_GEOGFROMEWKT(EXCLUDED.location::TEXT),
            osm = EXCLUDED.osm::JSONB,
            updated_at = NOW();
    """)
    crdb.execute("DROP TABLE IF EXISTS databot.places_osm_load;")


@flow
def import_osm_places(country: list[str], **kwargs):
    """
    This flow imports the OSM places data.
    """
    log = get_logger()

    country = country[0]
    if not country:
        log.error("No country specified.")
        return

    log.info(f"Importing place data for {country.upper()}...")
    download_url = (
        f"https://download.geofabrik.de/europe/{country.lower()}-latest.osm.pbf"
    )
    filepath = load_osm(country, download_url)
    transform_osm(filepath)


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "country",
            type=str,
            nargs="+",
            help="The country to process.",
        )

    setup_cli(import_osm_places, args)
