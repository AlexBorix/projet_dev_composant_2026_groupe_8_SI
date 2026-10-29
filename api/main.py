import os

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException

app = FastAPI(title="API ventes validees", version="1.0")


def rows(sql: str, params=()):
    with psycopg2.connect(os.environ["PG_DSN"]) as c, c.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/sales")
def sales():
    """Ventes validees (produites par l'asset validated_sales)."""
    data = rows("SELECT * FROM public.validated_sales ORDER BY id")
    return {"count": len(data), "sales": data}


@app.get("/sales/{sale_id}")
def sale(sale_id: int):
    data = rows("SELECT * FROM public.validated_sales WHERE id = %s", (sale_id,))
    if not data:
        raise HTTPException(404, "Vente introuvable (absente ou non validee)")
    return data[0]


@app.get("/quality/latest")
def quality_latest():
    """Resultat du dernier run ayant enregistre les trois controles."""
    last = rows(
        "SELECT run_id FROM public.quality_results "
        "GROUP BY run_id "
        "HAVING COUNT(DISTINCT check_name) = 3 "
        "ORDER BY MAX(checked_at) DESC, MAX(id) DESC LIMIT 1"
    )
    if not last:
        raise HTTPException(404, "Aucun controle execute pour l'instant")
    run_id = last[0]["run_id"]
    checks = rows(
        "SELECT check_name, passed, metadata, checked_at FROM public.quality_results "
        "WHERE run_id = %s ORDER BY id",
        (run_id,),
    )
    return {
        "run_id": run_id,
        "all_passed": all(c["passed"] for c in checks),
        "checked_at": max(c["checked_at"] for c in checks),
        "checks": checks,
    }
