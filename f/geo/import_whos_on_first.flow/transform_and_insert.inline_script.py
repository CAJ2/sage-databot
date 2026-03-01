# requirements: project

from wmill import S3Object
import polars as pl
import polars.selectors as cs
import json
from sqlalchemy import text

from f.utils.s3 import S3Client
from f.utils.db.crdb import create_polars_uri, create_sql_engine

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


def main(s3_file: S3Object):
    """
    Transform the Whos On First data.
    """
    s3 = S3Client()
    filepath = "/wof.db"
    with open(filepath, "wb") as fp:
        s3.s3_download(s3_file["s3"], fp)

    names_df = pl.read_database_uri(
        query="SELECT id, placetype, language, script, region, name FROM names WHERE privateuse = 'preferred' "
        + f"AND placetype IN ({', '.join([f"'{i[0]}'" for i in placetype_admin])})",
        uri=f"sqlite://{filepath}",
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
    lang_df = lang_df.drop(cs.exclude(["id", "placetype", "name"]))
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
        uri=f"sqlite://{filepath}",
        engine="connectorx",
    )
    geojson_df = geojson_df.with_columns(
        pl.col("body").str.json_path_match("$.geometry").alias("geo"),
        pl.col("body").str.json_path_match("$.properties").alias("properties"),
    )
    geojson_df = geojson_df.drop("body")

    def filter_props(props: str) -> str | None:
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
    print(f"Total records: {combined_df.height}")

    conn = create_polars_uri()
    crdb = create_sql_engine()

    combined_df.write_database(
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
        )
        c.execute(text("DROP TABLE IF EXISTS databot.regions_wof_load"))
        c.execute(
            text(
                "UPDATE regions SET \"name\" = jsonb_set(\"name\", '{xx}', properties->'wof:name')"
            )
        )
