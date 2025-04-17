def generate_name(tags) -> dict:
    """
    Generate a name from the tags.

    TODO: Add support for translations.
    """
    name = {}
    for t in tags:
        if t.k.startswith("name"):
            p = t.k.split(":")
            lang = "xx"
            if len(p) == 2:
                lang = p[1]
            name[lang] = t.v
    if len(name.keys()) == 0:
        if "amenity" in tags:
            if (
                tags.get("amenity") == "recycling"
                and tags.get("recycling_type") == "centre"
            ):
                name["en"] = "Recycling Center"
                name["en-GB"] = "Recycling Centre"
            elif (
                tags.get("amenity") == "recycling"
                and tags.get("recycling_type") == "container"
            ):
                name["en"] = "Recycling Container"
            else:
                name["en"] = tags.get("amenity").replace("_", " ").title()
        else:
            name["en"] = "Place"

    return name


def generate_address(tags) -> dict:
    """
    Generate an address from the tags.

    TODO: Set language based on the country.
    """
    addr = {}
    if "addr:housenumber" in tags:
        addr["housenumber"] = tags.get("addr:housenumber")
    if "addr:street" in tags:
        addr["street"] = tags.get("addr:street")
    if "addr:postcode" in tags:
        addr["postcode"] = tags.get("addr:postcode")
    if "addr:city" in tags:
        addr["city"] = tags.get("addr:city")
    if len(addr.keys()) == 0:
        return None
    return {"xx": addr}
