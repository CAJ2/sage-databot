# requirements: project

from typing import cast
import os
import polars as pl
import osmium
from osmium import osm
import json
from sqlalchemy import text
from urllib.request import urlretrieve

from f.openstreetmap.generators import generate_name, generate_address
from f.openstreetmap.osm_tags import waste_tags
from f.utils.db.crdb import create_polars_uri, create_sql_engine


def load_osm(country: str, download_url: str):
    """
    Download the OSM country data from the given URL.
    """
    # Download the file
    filename = os.path.basename(download_url)
    filepath = os.path.join(".", filename)

    if not os.path.exists(filepath):
        print(f"Downloading {download_url} to {filepath}")
        urlretrieve(download_url, filepath)
    else:
        print(f"File {filepath} already exists, skipping download.")
    print(f"Downloaded country {country} to {filepath}")

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


def transform_osm(filepath: str):
    """
    Transform the OSM data.
    """

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
        osmium.FileProcessor(filepath)
        .with_locations()
        .with_filter(osmium.filter.TagFilter(*waste_tags))  # type: ignore
    )
    rows = []
    total = 0
    for o in pbf:
        o = cast(osm.OSMObject, o)
        if o.is_node():
            o = cast(osm.Node, o)
            node = (
                f"node_{o.id}",
                json.dumps(generate_name(o.tags), ensure_ascii=False),
                json.dumps(generate_address(o.tags), ensure_ascii=False),
                f"SRID=4326;POINT({o.location.lon} {o.location.lat})",
                construct_osm_json(o),
            )
            rows.append(node)
        if o.is_way():
            o = cast(osm.Way, o)
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
            print(f"Processed {total} rows...")
            places_df = places_df.vstack(pl.DataFrame(rows, schema=places_df.schema))
            rows = []
    if len(rows) > 0:
        total += len(rows)
        print(f"Processed {total} rows...")
        places_df = places_df.vstack(
            pl.DataFrame(rows, schema=places_df.schema, orient="row")
        )
        rows = []

    crdb = create_sql_engine()
    conn = create_polars_uri()

    places_df.write_database(
        connection=conn,
        table_name="databot.places_osm_load",
        if_table_exists="replace",
        engine="adbc",
    )

    with crdb.begin() as conn:
        conn.execute(
            text("ALTER TABLE databot.places_osm_load ALTER COLUMN id SET NOT NULL;")
        )
        conn.execute(
            text(
                "ALTER TABLE databot.places_osm_load ALTER PRIMARY KEY USING COLUMNS (id);"
            )
        )
        conn.execute(
            text("""
            INSERT INTO public.places (id, created_at, updated_at, name, address, location, osm)
            SELECT id, NOW(), NOW(), name::JSONB, address::JSONB, ST_GEOGFROMEWKT(location::TEXT), osm::JSONB
            FROM databot.places_osm_load
            ON CONFLICT (id) DO UPDATE
            SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                address = JSON_STRIP_NULLS(EXCLUDED.address::JSONB),
                location = EXCLUDED.location,
                osm = EXCLUDED.osm::JSONB,
                updated_at = NOW();
        """)
        )
        conn.execute(text("DROP TABLE IF EXISTS databot.places_osm_load;"))


def import_osm_places(continent: str, country: str):
    """
    This flow imports the OSM places data.
    """

    if continent.lower() not in [
        "africa",
        "asia",
        "australia-oceania",
        "central-america",
        "europe",
        "north-america",
        "south-america",
    ]:
        print(f"Invalid continent: {continent}")
        return
    if not country:
        print("No country specified.")
        return

    print(f"Importing place data for {country.upper()}...")
    download_url = f"https://download.geofabrik.de/{continent.lower()}/{country.lower()}-latest.osm.pbf"
    filepath = load_osm(country, download_url)
    transform_osm(filepath)


def main(continent: str, country: str):
    import_osm_places(continent, country)
