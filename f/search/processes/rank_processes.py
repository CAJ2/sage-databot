# requirements: project

import json

from sqlalchemy import Engine, text

from f.utils.db.crdb import create_sql_engine

WEIGHTS = {
    "has_name_en": 0.25,
    "has_desc_en": 0.20,
    "has_material_or_variant": 0.25,
    "has_efficiency": 0.15,
    "has_instructions": 0.10,
    "has_sources": 0.05,
}


def query_qual(crdb: Engine, ids: list[str]) -> dict[str, dict[str, bool]]:
    if not ids:
        return {}
    ids_join = "','".join(ids)
    with crdb.connect() as conn:
        rows = conn.execute(
            text(
                f"""SELECT id,
                (name->>'en' IS NOT NULL AND LENGTH(name->>'en') > 0) AS has_name_en,
                ("desc"->>'en' IS NOT NULL AND LENGTH("desc"->>'en') > 0) AS has_desc_en,
                (material_id IS NOT NULL OR variant_id IS NOT NULL) AS has_material_or_variant,
                (efficiency IS NOT NULL) AS has_efficiency,
                (instructions IS NOT NULL AND instructions::text != '{{}}') AS has_instructions,
                ((SELECT count(*) FROM public.process_sources WHERE process_id = processes.id) > 0) AS has_sources
                FROM public.processes WHERE id IN ('{ids_join}')"""
            )
        ).fetchall()
    return {
        str(row[0]): {
            "has_name_en": bool(row[1]),
            "has_desc_en": bool(row[2]),
            "has_material_or_variant": bool(row[3]),
            "has_efficiency": bool(row[4]),
            "has_instructions": bool(row[5]),
            "has_sources": bool(row[6]),
        }
        for row in rows
    }


def main(keys: list[str]) -> dict[str, int]:
    if not keys:
        return {"updated": 0}
    crdb = create_sql_engine()
    qual_by_id = query_qual(crdb, keys)

    ranks: dict[str, dict[str, float]] = {}
    for id_ in keys:
        signals = qual_by_id.get(id_, {})
        qual = sum(WEIGHTS[k] * (1.0 if v else 0.0) for k, v in signals.items())
        ranks[id_] = {
            "qual": round(qual, 4),
            "pop": 0.0,
            "order": round(qual, 4),
        }

    with crdb.begin() as conn:
        conn.execute(
            text("UPDATE public.processes SET rank = :rank WHERE id = :id"),
            [{"rank": json.dumps(r), "id": id_} for id_, r in ranks.items()],
        )

    return {"updated": len(ranks)}
