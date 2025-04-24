"""
Place Tags Template:
id: Nano ID
name: Translated name
type: PLACE
desc: Translated description
meta_template: Object containing a JSON schema and a UI schema
bg_color: Background color for the tag
image: Remote path to the image file
tag_id: Stable identifier for the tag
"""

place_tags = [
    {
        "id": "6LsWZmITO4_YHFnZcRvph",
        "name": {"en": "Opening Hours", "sv": "Öppettider"},
        "type": "PLACE",
        "desc": None,
        "meta_template": {
            "schema": {
                "$schema": "http://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "opening_hours": {
                        "type": "string",
                    },
                },
            },
            "ui_schema": {
                "type": "Control",
                "scope": "#/properties/opening_hours",
                "label": "Opening Hours",
            },
        },
        "bg_color": "#FF0000",
        "image": "iconify://mdi:clock",
        "tag_id": "opening_hours",
    },
]
