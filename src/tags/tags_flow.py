from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
import json
from jsonschema import Draft202012Validator

import src.tags.variant_tags as variant_tags
import src.tags.component_tags as component_tags
import src.tags.place_tags as place_tags


@flow
def update_db_tags():
    """
    Flow to update the tags table with predefined tags.
    """
    crdb = SqlAlchemyConnector.load("crdb-sage")

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
                with open("src/tags/" + meta_template["schema"], "r") as f:
                    meta_template["schema"] = json.load(f)
                    Draft202012Validator(meta_template["schema"]).check_schema(
                        meta_template["schema"]
                    )
            meta_template["uischema"] = tag["meta_template"]["uischema"]
            if type(meta_template["uischema"]) is str:
                with open("src/tags/" + meta_template["uischema"], "r") as f:
                    meta_template["uischema"] = json.load(f)

        crdb.execute(
            """
            INSERT INTO tags (id, created_at, updated_at, name, type, "desc", meta_template, bg_color, image, tag_id)
            VALUES (:id, NOW(), NOW(), :name, :type, :desc, :meta_template, :bg_color, :image, :tag_id)
            ON CONFLICT (type, tag_id) DO UPDATE
            SET name = JSON_STRIP_NULLS(EXCLUDED.name::JSONB),
                updated_at = NOW(),
                "desc" = JSON_STRIP_NULLS(EXCLUDED.desc::JSONB),
                meta_template = EXCLUDED.meta_template::JSONB,
                bg_color = EXCLUDED.bg_color,
                image = EXCLUDED.image
            """,
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


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    update_db_tags()
