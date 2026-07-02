# requirements: project

import json
import math

import duckdb
import wmill
from sqlalchemy import Engine, text

from f.utils.db.crdb import create_sql_engine

WEIGHTS = {
    "has_name_en": 0.25,
    "has_desc_en": 0.20,
    "has_location": 0.25,
    "has_address": 0.15,
    "has_org": 0.15,
}


def query_qual(crdb: Engine, ids: list[str]) -> dict[str, dict[str, bool]]:
    if not ids:
        return {}
    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                f"""SELECT id,
                (name->>'en' IS NOT NULL) AS has_name_en,
                ("desc"->>'en' IS NOT NULL) AS has_desc_en,
                (location IS NOT NULL) AS has_location,
                (address IS NOT NULL AND address::text != '{{}}') AS has_address,
                (org IS NOT NULL) AS has_org
                FROM public.places WHERE id IN ('{ids_join}')"""
            )
        ).fetchall()
    return {
        str(row[0]): {
            "has_name_en": bool(row[1]),
            "has_desc_en": bool(row[2]),
            "has_location": bool(row[3]),
            "has_address": bool(row[4]),
            "has_org": bool(row[5]),
        }
        for row in rows
    }


def query_pop(entity_type: str, ids: list[str]) -> dict[str, int]:
    if not ids:
        return {}
    try:
        conn = duckdb.connect()
        dl_settings = wmill.ducklake()
        conn.execute(f"ATTACH '{dl_settings}' AS dl")
        result = conn.execute(
            """
            SELECT entity_id, SUM(count)::INTEGER AS views
            FROM dl.main.hourly_entity_views
            WHERE entity_type = ? AND entity_id IN (SELECT UNNEST(?))
            GROUP BY entity_id
            """,
            [entity_type, ids],
        ).fetchall()
        return {row[0]: row[1] for row in result}
    except Exception:
        return {}


def normalize_pop(views: int, scale: int = 10_000) -> float:
    return min(1.0, math.log1p(views) / math.log1p(scale))


def main(keys: list[str]) -> dict[str, int]:
    if not keys:
        return {"updated": 0}
    crdb = create_sql_engine()
    qual_by_id = query_qual(crdb, keys)
    views_by_id = query_pop("place", keys)

    ranks: dict[str, dict[str, float]] = {}
    for id_ in keys:
        signals = qual_by_id.get(id_, {})
        qual = sum(WEIGHTS[k] * (1.0 if v else 0.0) for k, v in signals.items())
        views = views_by_id.get(id_, 0)
        pop = normalize_pop(views)
        order = 0.4 * pop + 0.6 * qual
        ranks[id_] = {
            "qual": round(qual, 4),
            "pop": round(pop, 4),
            "order": round(order, 4),
        }

    with crdb.begin() as conn:
        conn.execute(
            text("UPDATE public.places SET rank = :rank WHERE id = :id"),
            [{"rank": json.dumps(r), "id": id_} for id_, r in ranks.items()],
        )

    return {"updated": len(ranks)}
