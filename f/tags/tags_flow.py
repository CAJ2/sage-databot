# requirements: project

from pathlib import Path
from sqlalchemy import text
import json
from jsonschema import Draft202012Validator

import f.tags.variant_tags as variant_tags
import f.tags.component_tags as component_tags
import f.tags.place_tags as place_tags
from f.utils.git import checkout_repo
from f.utils.db.crdb import create_sql_engine


def update_db_tags(repo_path: Path):
    """
    Flow to update the tags table with predefined tags.
    """
    crdb = create_sql_engine()

    all_tags = []
    for tags in [variant_tags.tags, component_tags.tags, place_tags.tags]:
        all_tags.extend(tags)

    # Iterate over each tag and upsert it into the database
    for tag in all_tags:
        meta_template = None
        if "meta_template" in tag and tag["meta_template"] is not None:
            meta_template = {}
            meta_template["schema"] = tag["meta_template"]["schema"]
            if type(meta_template["schema"]) is str:
                with open(
                    repo_path / "src" / "tags" / meta_template["schema"], "r"
                ) as f:
                    meta_template["schema"] = json.load(f)
                    Draft202012Validator(meta_template["schema"]).check_schema(
                        meta_template["schema"]
                    )
            meta_template["uischema"] = tag["meta_template"]["uischema"]
            if type(meta_template["uischema"]) is str:
                with open(
                    repo_path / "src" / "tags" / meta_template["uischema"], "r"
                ) as f:
                    meta_template["uischema"] = json.load(f)

        with crdb.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO tags (id, created_at, updated_at, name, type, "desc", meta_template, bg_color, image, tag_id)
                VALUES (:id, NOW(), NOW(), :name, :type, :desc, :meta_template, :bg_color, :image, :tag_id)
                ON CONFLICT (type, tag_id) DO UPDATE
                SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                    updated_at = NOW(),
                    "desc" = JSON_STRIP_NULLS(EXCLUDED.desc::JSONB),
                    meta_template = EXCLUDED.meta_template::JSONB,
                    bg_color = EXCLUDED.bg_color,
                    image = EXCLUDED.image
                """),
                {
                    "id": tag["id"],
                    "name": json.dumps(tag["name"], ensure_ascii=False),
                    "type": tag["type"],
                    "desc": json.dumps(tag["desc"], ensure_ascii=False),
                    "meta_template": json.dumps(meta_template, ensure_ascii=False),
                    "bg_color": tag["bg_color"],
                    "image": tag["image"],
                    "tag_id": tag["tag_id"],
                },
            )


def main():
    repo_path = checkout_repo()
    update_db_tags(repo_path)
    print("Tags updated successfully.")
