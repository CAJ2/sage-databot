# requirements: project

from typing import Any

from sqlalchemy import JSON, select, text
from sqlalchemy.orm import Mapped, mapped_column
import json
from jsonschema import validate

from f.utils.db.crdb import create_sql_engine, Base


class Tags(Base):
    __tablename__: str = "tags"

    id: Mapped[str] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column()
    meta_template: Mapped[dict[str, Any]] = mapped_column(JSON)
    tag_id: Mapped[str] = mapped_column()


def process_tag(tag_defs: dict[str, Tags], t: str, v: str) -> tuple[str, str] | None:
    """
    Check if a tag relation should be created.
    """
    if t == "opening_hours":
        tag_tmpl = tag_defs.get("opening_hours")
        if tag_tmpl:
            meta = None
            if "schema" in tag_tmpl.meta_template:
                meta = {"opening_hours": v}
                try:
                    validate(meta, tag_tmpl.meta_template["schema"])
                except Exception:
                    return None
            return (tag_tmpl.tag_id, json.dumps(meta, ensure_ascii=False))
    return None


def osm_tags():
    """
    Assign Sage database place tags based on OSM tags.
    """

    crdb = create_sql_engine()

    with crdb.begin() as conn:
        # Fetch all tags from the database
        stmt = select(Tags).where(Tags.type == "PLACE")
        tags_cur = conn.execute(stmt)
        tag_defs = dict((row.tag_id, row) for row in tags_cur.scalars())

        # Scan the places table for OSM tags by parsing the osm column
        places_cur = conn.execute(
            text("SELECT id, osm FROM places WHERE osm IS NOT NULL"),
            {"yield_per": 1000},
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
                print(f"Found {total_rel} tag relations...")
                with crdb.begin() as conn:
                    conn.execute(
                        text("""
                        UPSERT INTO places_tags (place_id, tag_id, meta)
                        VALUES (:place_id, :tag_id, :meta)
                        """),
                        relations,
                    )
                relations = []
        if len(relations) > 0:
            total_rel += len(relations)
            print(f"Found {total_rel} tag relations...")
            with crdb.begin() as conn:
                conn.execute(
                    text("""
                    UPSERT INTO places_tags (place_id, tag_id, meta)
                    VALUES (:place_id, :tag_id, :meta)
                    """),
                    relations,
                )
            relations = []
    print("Finished processing all places.")


def main():
    osm_tags()
