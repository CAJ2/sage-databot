"""
Tags Template:
id: Nano ID
tag_id: Stable identifier for the tag
name: Translated name
type: VARIANT
desc: Translated description
meta_template: Object containing a JSON schema and a UI schema
bg_color: Background color for the tag
image: Remote path to the image file
"""

tags = [
    {
        "id": "9ytcoER6E3yFSQz6o2JZC",
        "tag_id": "origins",
        "name": {
            "en": "Origins, Manufacturing, and Distribution",
            "sv": "Ursprung, Tillverkning och Distribution",
        },
        "type": "VARIANT",
        "desc": None,
        "meta_template": {
            "schema": "schemas/variant_origins.json",
            "ui_schema": "ui_schemas/variant_origins.json",
        },
        "bg_color": "#FF0000",
        "image": "iconify://mdi:building",
    },
]
