from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
import json
from jsonschema import validate

from src.utils.logging.loggers import get_logger


def process_tag(tag_defs: dict[str, tuple], t: str, v: str) -> tuple[str, dict]:
    """
    Check if a tag relation should be created.
    """
    if t == "opening_hours":
        tag_tmpl = tag_defs.get("opening_hours")
        if tag_tmpl:
            meta = None
            if "schema" in tag_tmpl[1]:
                meta = {"opening_hours": v}
                try:
                    validate(meta, tag_tmpl[1]["schema"])
                except Exception:
                    return None
            return (tag_tmpl[0], json.dumps(meta, ensure_ascii=False))
    return None


@flow
def osm_tags_flow():
    """
    Flow to assign Sage database place tags based on OSM tags.
    """
    log = get_logger()

    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Fetch all tags from the database
    tags_cur = crdb.fetch_all(
        "SELECT id, meta_template, tag_id FROM tags WHERE type = 'PLACE'",
    )
    tag_defs = dict((row[2], row) for row in tags_cur)

    # Scan the places table for OSM tags by parsing the osm column
    places_cur = crdb.fetch_all(
        "SELECT id, osm FROM places WHERE osm IS NOT NULL", {"yield_per": 1000}
    )
    relations = []
    total_rel = 0
    for row in places_cur:
        osm = row[1]
        for t, v in osm["tags"].items():
            relation = process_tag(tag_defs, t, v)
            if relation:
                relations.append(
                    {"place_id": row[0], "tag_id": relation[0], "meta": relation[1]}
                )
        if len(relations) >= 1000:
            total_rel += len(relations)
            log.info(f"Found {total_rel} tag relations...")
            crdb.execute(
                """
                UPSERT INTO places_tags (place_id, tag_id, meta)
                VALUES (:place_id, :tag_id, :meta)
                """,
                relations,
            )
            relations = []
    if len(relations) > 0:
        total_rel += len(relations)
        log.info(f"Found {total_rel} tag relations...")
        crdb.execute(
            """
            UPSERT INTO places_tags (place_id, tag_id, meta)
            VALUES (:place_id, :tag_id, :meta)
            """,
            relations,
        )
        relations = []
    log.info("Finished processing all places.")
    crdb.close()


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    osm_tags_flow()
