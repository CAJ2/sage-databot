from prefect import flow
from prefect.variables import Variable
from prefect_sqlalchemy import SqlAlchemyConnector
import polars as pl
import polars.selectors as cs
import meilisearch
from stopwordsiso import stopwords
import json
import iso639
import copy

from src.cli import setup_cli
from src.utils.logging.loggers import get_logger

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


def check_create_index(meili: meilisearch.Client, index_name: str, settings: dict = {}):
    op = meili.create_index(index_name, {"primaryKey": "id"})
    meili.wait_for_task(op.task_uid)
    settings_copy = copy.deepcopy(index_settings)
    settings_copy.update(settings)
    op = meili.index(index_name).update_settings(settings_copy)
    meili.wait_for_task(op.task_uid)


def export_table(
    crdb: SqlAlchemyConnector, table: str, cols: str = "*", schema: dict = None
) -> pl.DataFrame:
    """
    Export a table from the database to a Polars DataFrame.
    """
    records = crdb.fetch_all(
        f"SELECT {cols} FROM {table}",
    )
    df = pl.DataFrame(records, schema_overrides=schema, orient="row")
    return df


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
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the regions in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.regions",
        cols="id, name::string, properties::string, placetype",
    )
    log.info(f"Exported {df.height} rows from public.regions")
    log.info(f"Columns: {df.describe()}")
    df = df.cast({pl.Datetime: pl.String})
    docs = df.to_dicts()
    for doc in docs:
        name_json = json.loads(doc["name"])
        doc["name"] = name_json
        prop_json = json.loads(doc["properties"])
        doc["properties"] = prop_json
    meili.index("regions").add_documents(docs)


def index_orgs(
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the organizations in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.orgs",
        cols='id, updated_at, name, "desc"::string, avatar_url',
    )
    log.info(f"Exported {df.height} rows from public.orgs")
    log.info(f"Columns: {df.describe()}")
    df = df.cast({pl.Datetime: pl.String})
    docs = df.to_dicts()
    for doc in docs:
        desc_json = json.loads(doc["desc"])
        doc["desc"] = desc_json
    meili.index("orgs").add_documents(docs)


def index_categories(
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the categories in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.categories",
        cols='id, name::string, desc_short::string, "desc"::string, image_url',
    )
    log.info(f"Exported {df.height} rows from public.categories")
    log.info(f"Columns: {df.describe()}")
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
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the variants in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.variants",
        cols='id, updated_at, name::string, "desc"::string, code',
    )
    log.info(f"Exported {df.height} rows from public.variants")
    log.info(f"Columns: {df.describe()}")
    df = df.cast({pl.Datetime: pl.String})
    docs = df.to_dicts()
    for doc in docs:
        name_json = json.loads(doc["name"])
        doc["name"] = name_json
        desc_json = json.loads(doc["desc"] or "{}")
        doc["desc"] = desc_json
    meili.index("variants").add_documents(docs)


def index_materials(
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the materials in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.materials",
        cols='id, name::string, "desc"::string, technical',
    )
    tree_df = export_table(
        crdb,
        "public.material_tree",
        cols="ancestor_id, descendant_id, depth",
    )
    log.info(f"Exported {df.height} rows from public.materials")
    log.info(f"Columns: {df.describe()}")
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


def index_places(
    crdb: SqlAlchemyConnector,
    meili: meilisearch.Client,
):
    """
    Index the places in Meilisearch.
    """
    log = get_logger()
    df = export_table(
        crdb,
        "public.places",
        cols='id, updated_at, name::string, address::string, "desc"::string',
    )
    log.info(f"Exported {df.height} rows from public.places")
    log.info(f"Columns: {df.describe()}")
    df = df.cast({pl.Datetime: pl.String}).with_columns(cs.string().str.strip_chars())
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
    meili.index("places").add_documents(docs)


@flow
def search_index_import(index: list[str], clear: bool, **kwargs):
    """
    Import all database data into Meilisearch indexes.
    """
    log = get_logger()
    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Connect to Meilisearch
    meili = meilisearch.Client(
        Variable.get("meilisearch", default="http://localhost:7700"),
    )

    indexes = meili.get_indexes()
    index_uids = [index.uid for index in indexes["results"]]
    log.info(f"Meilisearch indexes: {index_uids}")

    if not index or "regions" in index:
        # Region index
        if clear:
            log.info("Clearing regions index")
            meili.index("regions").delete()
            index_uids.remove("regions")
        if "regions" not in index_uids:
            check_create_index(
                meili,
                "regions",
                {
                    "searchableAttributes": ["name", "properties"],
                    "filterableAttributes": ["placetype"],
                },
            )
        index_regions(crdb, meili)
    if not index or "orgs" in index:
        # Org index
        if clear:
            log.info("Clearing orgs index")
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
            log.info("Clearing categories index")
            meili.index("categories").delete()
            index_uids.remove("categories")
        if "categories" not in index_uids:
            check_create_index(
                meili,
                "categories",
                {"searchableAttributes": ["name", "desc_short", "desc"]},
            )
        index_categories(crdb, meili)
    # if not index or "items" in index:
    # Item index
    # if "items" not in index_uids:
    #     check_create_index(meili, "items")
    #     index_items(crdb, meili)
    if not index or "variants" in index:
        # Variant index
        if clear:
            log.info("Clearing variants index")
            meili.index("variants").delete()
            index_uids.remove("variants")
        if "variants" not in index_uids:
            check_create_index(
                meili, "variants", {"searchableAttributes": ["name", "desc", "code"]}
            )
        index_variants(crdb, meili)
    # if not index or "components" in index:
    # Component index
    # if "components" not in index_uids:
    #     check_create_index(meili, "components")
    #     index_components(crdb, meili)
    if not index or "materials" in index:
        # Material index
        if clear:
            log.info("Clearing materials index")
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
            log.info("Clearing places index")
            meili.index("places").delete()
            index_uids.remove("places")
        if "places" not in index_uids:
            check_create_index(
                meili,
                "places",
                {"searchableAttributes": ["name", "address", "desc"]},
            )
        index_places(crdb, meili)


if __name__ == "__main__":

    def args(parser):
        parser.add_argument(
            "--clear",
            action="store_true",
            default=False,
            help="Clear all indexes before importing",
        )
        parser.add_argument(
            "index",
            nargs="+",
            help="Select a specific index to import",
        )

    setup_cli(search_index_import, args)
