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
            "en": "Places of Origin",
            "sv": "Ursprungsplatser",
        },
        "type": "VARIANT",
        "desc": None,
        "meta_template": {
            "schema": "schemas/variant_origins.json",
            "uischema": "ui_schemas/variant_origins.json",
        },
        "bg_color": "#92B4A7",
        "image": "iconify://ic:baseline-place",
    },
    {
        "id": "bnsXi41vh9qb5SudBKGWR",
        "tag_id": "manufacturing",
        "name": {
            "en": "Manufacturing",
            "sv": "Tillverkning",
        },
        "type": "VARIANT",
        "desc": None,
        "meta_template": {
            "schema": "schemas/variant_manufacturing.json",
            "uischema": "ui_schemas/variant_manufacturing.json",
        },
        "bg_color": "#2F2F2F",
        "image": "iconify://mdi:building",
    },
    {
        "id": "pf77bSePb7M0IgzLJfkTh",
        "tag_id": "organic",
        "name": {
            "en": "Certified Organic",
            "sv": "Certifierat Ekologiskt",
        },
        "type": "VARIANT",
        "desc": None,
        "meta_template": None,
        "bg_color": "#69B578",
        "image": "iconify://mdi:organic",
    },
]
