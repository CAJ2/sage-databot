# requirements: project
import json
import re
from typing import Any

from sqlalchemy import text

from f.utils.db.crdb import create_sql_engine

# Latin alphabet + diacritics + standard punctuation
LATIN_REGEX = re.compile(r"^[a-zA-Z\u00C0-\u024F\u1E00-\u1EFF\s\-.,'’()]+$")


def is_latin(s: str) -> bool:
    if not s:
        return False
    return bool(LATIN_REGEX.match(s))


def parse_json(val: Any) -> Any:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, str):  # Handle double-encoding
                return json.loads(parsed)
            return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def main(last_id: str = ""):
    engine = create_sql_engine()
    name_cache: dict[str | int, dict[str, str]] = {}

    def get_name_dict(region_id: str | int, conn: Any) -> dict[str, str]:
        if region_id in name_cache:
            return name_cache[region_id]

        db_id = str(region_id)
        if db_id.isdigit():
            db_id = f"wof_{db_id}"

        result = conn.execute(
            text("SELECT name FROM public.regions WHERE id = :id"), {"id": db_id}
        ).fetchone()

        name_dict = parse_json(result[0]) if result else {}
        if not isinstance(name_dict, dict):
            name_dict = {}
        name_cache[region_id] = name_dict
        return name_dict

    batch_size = 1000
    while True:
        with engine.begin() as conn:
            # Using id > last_id for efficient pagination on large table
            rows = (
                conn.execute(
                    text(
                        "SELECT id, name, properties FROM public.regions WHERE id > :last_id ORDER BY id LIMIT :limit"
                    ),
                    {"limit": batch_size, "last_id": last_id},
                )
                .mappings()
                .all()
            )

            if not rows:
                break

            for row in rows:
                region_id = row["id"]
                last_id = region_id
                name = parse_json(row["name"])
                properties = parse_json(row["properties"])

                # 1. Fix missing 'eng'
                if "eng" not in name or not name["eng"]:
                    for lang, val in name.items():
                        if is_latin(val):
                            name["xx"] = val
                            break

                # 2. Generate hierarchy description
                hierarchy_data = properties.get("hierarchy", [])
                hierarchy_ids = []

                if isinstance(hierarchy_data, list) and hierarchy_data:
                    # Format A: [{"id": 123, "admin_level": 11, "placetype": "locality"}, ...]
                    if (
                        len(hierarchy_data) > 0
                        and isinstance(hierarchy_data[0], dict)
                        and "id" in hierarchy_data[0]
                    ):
                        hierarchy_ids = [
                            item["id"]
                            for item in hierarchy_data
                            if "id" in item and f"wof_{item['id']}" != region_id
                        ]
                    # Format B: [{"continent_id": 123, "country_id": 456, ...}]
                    elif len(hierarchy_data) > 0 and isinstance(
                        hierarchy_data[0], dict
                    ):
                        h_dict = hierarchy_data[0]
                        order = [
                            "locality_id",
                            "localadmin_id",
                            "county_id",
                            "region_id",
                            "country_id",
                        ]
                        for key in order:
                            if key in h_dict and f"wof_{h_dict[key]}" != region_id:
                                hierarchy_ids.append(h_dict[key])

                hierarchy_names = []
                seen_ids = set()
                for h_id in hierarchy_ids:
                    if h_id in seen_ids:
                        continue
                    seen_ids.add(h_id)
                    h_name_dict = get_name_dict(h_id, conn)
                    if h_name_dict:
                        hierarchy_names.append(h_name_dict)

                if not hierarchy_names:
                    hierarchy_names = [name]

                all_langs = set()
                for hn in hierarchy_names:
                    all_langs.update(hn.keys())

                desc_dict = {}
                for lang in all_langs:
                    parts = []
                    for hn in hierarchy_names:
                        val = hn.get(lang) or hn.get("eng") or hn.get("xx")
                        if val and val not in parts:
                            parts.append(val)
                    if parts:
                        desc_dict[lang] = ", ".join(parts)

                conn.execute(
                    text(
                        'UPDATE public.regions SET "name" = :name, "desc" = :desc WHERE id = :id'
                    ),
                    {
                        "name": json.dumps(name, ensure_ascii=False),
                        "desc": json.dumps(desc_dict, ensure_ascii=False),
                        "id": region_id,
                    },
                )

        print(f"Processed batch ending at {last_id}")


if __name__ == "__main__":
    main()
