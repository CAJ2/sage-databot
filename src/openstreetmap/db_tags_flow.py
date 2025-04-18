from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
import json
import nanoid

from src.openstreetmap.db_tags import place_tags


@flow
def update_db_tags():
    """
    Flow to update the tags table with predefined tags in place_tags.
    """
    crdb = SqlAlchemyConnector.load("crdb-sage")

    # Iterate over each tag in place_tags and upsert it into the database
    for tag in place_tags:
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
                "id": nanoid.generate(),
                "name": json.dumps(tag["name"], ensure_ascii=False),
                "type": tag["type"],
                "desc": json.dumps(tag["desc"], ensure_ascii=False),
                "meta_template": json.dumps(tag["meta_template"], ensure_ascii=False),
                "bg_color": tag["bg_color"],
                "image": tag["image"],
                "tag_id": tag["tag_id"],
            },
        )


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    update_db_tags()
