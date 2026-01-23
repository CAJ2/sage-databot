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

tags = [
    {
        "id": "6LsWZmITO4_YHFnZcRvph",
        "name": {"en": "Opening Hours", "sv": "Öppettider"},
        "type": "PLACE",
        "desc": None,
        "meta_template": {
            "schema": "schemas/place_opening_hours.json",
            "uischema": "ui_schemas/place_opening_hours.json",
        },
        "bg_color": "#7236FD",
        "image": "icon://mdi:clock",
        "tag_id": "opening_hours",
    },
]
