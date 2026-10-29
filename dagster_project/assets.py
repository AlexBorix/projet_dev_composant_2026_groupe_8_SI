import dagster as dg

from .airbyte_sync import RAW_SALES_KEY
from .db import pg_conn


@dg.asset(
    deps=[RAW_SALES_KEY],
    group_name="serving",
    description="Copie des ventes valides de public.sales vers public.validated_sales. "
    "Ne s'execute que si les checks bloquants de raw_sales ont reussi.",
)
def validated_sales(context: dg.AssetExecutionContext):
    with pg_conn() as conn, conn.cursor() as cur:  # une seule transaction
        cur.execute("DELETE FROM public.validated_sales")
        cur.execute(
            """INSERT INTO public.validated_sales (id, customer, product, amount, sold_at)
               SELECT id, customer, product, amount, sold_at
               FROM public.sales
               WHERE id IS NOT NULL AND amount IS NOT NULL AND amount > 0"""
        )
        n = cur.rowcount
    context.log.info(f"{n} ventes validees")
    return dg.MaterializeResult(metadata={"validated_rows": n})
