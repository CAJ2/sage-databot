# requirements: project

import json
import tarfile
from wmill import S3Object
import polars as pl
from sqlalchemy import text

from f.utils.s3 import S3Client
from f.utils.db.crdb import create_polars_uri, create_sql_engine

placetype_admin = [["country", "2"]]


def parse_names(props: dict[str, object]) -> dict[str, str]:
    """
    Extract preferred names from legacy WOF properties.
    Keys are like name:eng_x_preferred, name:fra_x_preferred, name:cmn_Hans_x_preferred.
    Values are lists of strings.
    """
    names = {}
    for key, value in props.items():
        if not key.startswith("name:"):
            continue
        name_part = key[5:]  # Remove "name:" prefix
        if "_x_preferred" not in name_part:
            continue
        lang_part = name_part.split("_x_")[0]
        # Normalize underscores to hyphens for BCP47 format (e.g., cmn_Hans -> cmn-Hans)
        lang_normalized = lang_part.replace("_", "-")
        if isinstance(value, list) and len(value) > 0:
            names[lang_normalized] = value[0]
        elif isinstance(value, str) and value:
            names[lang_normalized] = value
    return names


def main(s3_file: S3Object):
    """
    Transform the Whos On First countries data from legacy GeoJSON tarball.
    """
    s3 = S3Client()
    filepath = "/wof_countries.tar.bz2"
    with open(filepath, "wb") as fp:
        s3.s3_download(s3_file["s3"], fp)

    allowed_placetypes = [i[0] for i in placetype_admin]
    placetype_to_admin = {i[0]: int(i[1]) for i in placetype_admin}

    records = []
    with tarfile.open(filepath, "r:*") as tar:
        for member in tar:
            if not member.name.endswith(".geojson"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            try:
                data = json.loads(f.read())
            except Exception:
                continue

            props = data.get("properties", {})
            placetype = props.get("wof:placetype")
            if placetype not in allowed_placetypes:
                continue

            wof_id = props.get("wof:id")
            if not wof_id:
                continue

            geo = data.get("geometry")
            if geo is None:
                continue

            names = parse_names(props)
            wof_name = props.get("wof:name")
            if wof_name:
                names["xx"] = wof_name

            # Build filtered properties (exclude name: and ne: prefixed keys)
            filtered_props: dict[str, object] = {}
            for k, v in props.items():
                if not (k.startswith("name:") or k.startswith("ne:")):
                    filtered_props[k] = v

            # Build hierarchy: countries only reference themselves
            admin_level = placetype_to_admin[placetype]
            filtered_props["hierarchy"] = [
                {"id": wof_id, "placetype": placetype, "admin_level": admin_level}
            ]

            records.append(
                {
                    "id": wof_id,
                    "placetype": placetype,
                    "admin_level": admin_level,
                    "name": json.dumps(names),
                    "geo": json.dumps(geo),
                    "properties": json.dumps(filtered_props),
                }
            )

    print(f"Total records: {len(records)}")
    if not records:
        print("No records found")
        return

    df = pl.DataFrame(
        records,
        schema={
            "id": pl.Int64,
            "placetype": pl.String,
            "admin_level": pl.Int64,
            "name": pl.String,
            "geo": pl.String,
            "properties": pl.String,
        },
    )

    conn = create_polars_uri()
    crdb = create_sql_engine()

    df.write_database(
        connection=conn,
        table_name="databot.regions_wof_load",
        if_table_exists="replace",
        engine="adbc",
    )

    with crdb.begin() as c:
        c.execute(
            text("ALTER TABLE databot.regions_wof_load ALTER COLUMN id SET NOT NULL")
        )
        c.execute(
            text(
                "ALTER TABLE databot.regions_wof_load ALTER PRIMARY KEY USING COLUMNS (id)"
            )
        )
        c.execute(
            text("""
            INSERT INTO public.regions (id, created_at, updated_at, name, geo, properties, placetype, admin_level)
            SELECT 'wof_' || id, NOW(), NOW(), JSON_STRIP_NULLS(name::JSONB),
                ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_SimplifyPreserveTopology(ST_GEOMFROMGEOJSON(geo::JSONB), 0.001)))),
                properties::JSONB, placetype, admin_level
            FROM databot.regions_wof_load
            ON CONFLICT (id) DO UPDATE
            SET placetype = EXCLUDED.placetype,
                name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                geo = ST_MULTIPOLYFROMWKB(ST_ASEWKB(ST_MULTI(ST_SimplifyPreserveTopology(ST_GEOMFROMGEOJSON(EXCLUDED.geo::JSONB), 0.001)))),
                properties = EXCLUDED.properties::JSONB,
                admin_level = EXCLUDED.admin_level,
                updated_at = NOW();
        """)
        )
        c.execute(text("DROP TABLE IF EXISTS databot.regions_wof_load"))
