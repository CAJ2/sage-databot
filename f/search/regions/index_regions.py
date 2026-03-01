# requirements: project

from sqlalchemy import Engine
import polars as pl
import meilisearch
import json

from f.utils.db.meili import meili_connect, check_create_index
from f.utils.db.crdb import create_sql_engine, export_table_by_ids


def index_regions(
    crdb: Engine,
    meili: meilisearch.Client,
    keys: list[str],
):
    """
    Index the regions in Meilisearch.
    """
    df_iter = export_table_by_ids(
        crdb,
        "public.regions",
        ids=keys,
        cols="id, name::string, properties::string, placetype, admin_level",
        batch_size=1000,
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.regions")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            prop_json = json.loads(doc["properties"])
            doc["properties"] = prop_json
            doc["_geo"] = {
                "lat": prop_json["geom:latitude"],
                "lng": prop_json["geom:longitude"],
            }
        meili.index("regions").add_documents(docs)


def main(keys: list[str]):
    crdb = create_sql_engine()
    meili = meili_connect()
    check_create_index(
        meili,
        "regions",
        {
            "searchableAttributes": ["name", "properties"],
            "filterableAttributes": ["placetype"],
            "sortableAttributes": ["admin_level"],
        },
    )
    index_regions(crdb, meili, keys)
