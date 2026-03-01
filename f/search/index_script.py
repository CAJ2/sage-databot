# requirements: project

from typing import Any, Iterator
import wmill
from sqlalchemy import Engine
import polars as pl
import polars.selectors as cs
import meilisearch
from stopwordsiso import stopwords
import json
import iso639
import copy

from f.utils.db.crdb import create_sql_engine

locales = ["en", "sv"]

index_settings = {
    "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness",
    ],
    "sortableAttributes": ["updated_at"],
    "stopWords": list(stopwords(locales)),
    "localizedAttributes": list(
        {"locales": [o], "attributePatterns": ["*." + o]} for o in locales
    ),
}


def check_create_index(
    meili: meilisearch.Client, index_name: str, settings: dict[str, Any] = {}
):
    op = meili.create_index(index_name, {"primaryKey": "id"})
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=500)
    settings_copy = copy.deepcopy(index_settings)
    settings_copy.update(settings)
    op = meili.index(index_name).update_settings(settings_copy)
    meili.wait_for_task(op.task_uid, timeout_in_ms=120000, interval_in_ms=5000)


def export_table(
    crdb: Engine,
    table: str,
    cols: str = "*",
    schema: dict[str, Any] | None = None,
    batch_size: int = 5000,
) -> Iterator[pl.DataFrame]:
    """
    Export a table from the database to a Polars DataFrame.
    """
    df_iter = pl.read_database(
        f"SELECT {cols} FROM {table}",
        connection=crdb,
        schema_overrides=schema,
        iter_batches=True,
        batch_size=batch_size,
    )
    return df_iter


def check_lang(lang: str):
    """
    Check if the language is valid.
    """
    try:
        if lang == "xx":
            return lang
        language = iso639.Language.match(lang.split("-")[0])
        if language.part1:
            lang = language.part1
        else:
            lang = language.part3
    except Exception:
        return None
    return lang


def index_regions(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the regions in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.regions",
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


def index_orgs(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the organizations in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.orgs",
        cols='id, updated_at, name, "desc"::string, avatar_url',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.orgs")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            desc_json = json.loads(doc["desc"])
            doc["desc"] = desc_json
        meili.index("orgs").add_documents(docs)


def index_categories(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the categories in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.categories",
        cols='id, name::string, desc_short::string, "desc"::string, image_url',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.categories")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        df = df.filter(pl.col("id").ne("CATEGORY_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            desc_short_json = json.loads(doc["desc_short"] or "{}")
            doc["desc_short"] = desc_short_json
        meili.index("categories").add_documents(docs)


def index_variants(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the variants in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.variants",
        cols='id, updated_at, name::string, "desc"::string, code',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.variants")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
        meili.index("variants").add_documents(docs)


def index_components(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the components in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.components",
        cols='id, updated_at, name::string, "desc"::string',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.components")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
        meili.index("components").add_documents(docs)


def index_materials(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the materials in Meilisearch.
    """
    df = export_table(
        crdb,
        "public.materials",
        cols='id, name::string, "desc"::string, technical',
        batch_size=10000,
    )
    tree_df = export_table(
        crdb,
        "public.material_tree",
        cols="ancestor_id, descendant_id, depth",
        batch_size=10000,
    )
    plus_one = False
    for df, tree_df in zip(df, tree_df):
        if plus_one:
            print("Material export resulted in multiple batches, increase batch size")
            return
        print(f"Exported {df.height} rows from public.materials")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String})
        df = df.filter(pl.col("id").ne("MATERIAL_ROOT"))
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
        for doc in docs:
            # Load all descendants to technical materials
            if not doc["technical"]:
                tree_df_filtered = tree_df.filter(
                    (pl.col("ancestor_id") == doc["id"]) & (pl.col("depth") > 0)
                )
                if tree_df_filtered.height > 0:
                    doc["technical_descendants"] = []
                for row in tree_df_filtered.iter_rows():
                    descendant_id = row[1]
                    for doc2 in docs:
                        if doc2["id"] == descendant_id and doc2["technical"]:
                            doc["technical_descendants"].append(doc2["name"])
        meili.index("materials").add_documents(docs)
        plus_one = True


def index_places(
    crdb: Engine,
    meili: meilisearch.Client,
):
    """
    Index the places in Meilisearch.
    """
    df_iter = export_table(
        crdb,
        "public.places",
        cols='id, updated_at, name::string, address::string, "desc"::string, st_asgeojson(location) as location',
    )
    for df in df_iter:
        print(f"Exported {df.height} rows from public.places")
        print(f"Columns: {df.describe()}")
        df = df.cast({pl.Datetime: pl.String}).with_columns(
            cs.string().str.strip_chars()
        )
        docs = df.to_dicts()
        for doc in docs:
            name_json = json.loads(doc["name"])
            doc["name"] = name_json
            if doc["address"].startswith(("None", "null", "NULL")):
                doc["address"] = "{}"
            address_json = json.loads(doc["address"] or "{}")
            doc["address"] = address_json
            desc_json = json.loads(doc["desc"] or "{}")
            doc["desc"] = desc_json
            geo_json = json.loads(doc["location"])
            doc["_geo"] = {
                "lat": geo_json["coordinates"][1],
                "lng": geo_json["coordinates"][0],
            }
            del doc["location"]
        meili.index("places").add_documents(docs)


def search_index_import(index: list[str] | None, clear: bool = False):
    """
    Import all database data into Meilisearch indexes.
    """
    crdb = create_sql_engine()

    # Connect to Meilisearch
    meili_res = wmill.get_resource("f/api_config/api_meilisearch")
    if meili_res is None:
        raise ValueError("Unable to find meilisearch resource")
    meili = meilisearch.Client(
        str(meili_res["api_url"]),
        api_key=meili_res.get("api_key", None),
    )

    indexes = meili.get_indexes()
    index_uids = [index.uid for index in indexes["results"]]
    print(f"Meilisearch indexes: {index_uids}")

    if not index or "regions" in index:
        # Region index
        if clear:
            print("Clearing regions index")
            meili.index("regions").delete()
            index_uids.remove("regions")
        if "regions" not in index_uids:
            check_create_index(
                meili,
                "regions",
                {
                    "searchableAttributes": ["name", "properties"],
                    "filterableAttributes": ["placetype"],
                    "sortableAttributes": ["admin_level"],
                },
            )
        index_regions(crdb, meili)
    if not index or "orgs" in index:
        # Org index
        if clear:
            print("Clearing orgs index")
            meili.index("orgs").delete()
            index_uids.remove("orgs")
        if "orgs" not in index_uids:
            check_create_index(
                meili, "orgs", {"searchableAttributes": ["name", "desc"]}
            )
        index_orgs(crdb, meili)
    if not index or "categories" in index:
        # Category index
        if clear:
            print("Clearing categories index")
            meili.index("categories").delete()
            index_uids.remove("categories")
        if "categories" not in index_uids:
            check_create_index(
                meili,
                "categories",
                {"searchableAttributes": ["name", "desc_short", "desc"]},
            )
        index_categories(crdb, meili)
    if not index or "items" in index:
        # Item index
        if "items" not in index_uids:
            check_create_index(
                meili, "items", {"searchableAttributes": ["name", "desc"]}
            )
        # index_items(crdb, meili)
    if not index or "variants" in index:
        # Variant index
        if clear:
            print("Clearing variants index")
            meili.index("variants").delete()
            index_uids.remove("variants")
        if "variants" not in index_uids:
            check_create_index(
                meili, "variants", {"searchableAttributes": ["name", "desc", "code"]}
            )
        index_variants(crdb, meili)
    if not index or "components" in index:
        # Component index
        if "components" not in index_uids:
            check_create_index(
                meili, "components", {"searchableAttributes": ["name", "desc"]}
            )
        index_components(crdb, meili)
    if not index or "materials" in index:
        # Material index
        if clear:
            print("Clearing materials index")
            meili.index("materials").delete()
            index_uids.remove("materials")
        if "materials" not in index_uids:
            check_create_index(
                meili,
                "materials",
                {"searchableAttributes": ["name", "desc", "technical_descendants"]},
            )
        index_materials(crdb, meili)
    if not index or "places" in index:
        # Place index
        if clear:
            print("Clearing places index")
            meili.index("places").delete()
            index_uids.remove("places")
        if "places" not in index_uids:
            check_create_index(
                meili,
                "places",
                {
                    "searchableAttributes": ["name", "address", "desc"],
                    "filterableAttributes": ["_geo"],
                },
            )
        index_places(crdb, meili)


def main(index: list[str] | None, clear: bool = False):
    if not index:
        search_index_import(index=None, clear=clear)
    else:
        search_index_import(index=index, clear=clear)
