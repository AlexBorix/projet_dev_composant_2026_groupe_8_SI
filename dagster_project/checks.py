"""Controles qualite executes APRES la synchronisation Airbyte.

Chaque check est attache a l'asset `raw_sales` avec blocking=True :
 - il ne s'execute qu'une fois `raw_sales` charge ;
 - un echec (severite ERROR) fait echouer le run et empeche `validated_sales`.
Chaque resultat est aussi journalise dans public.quality_results (lu par l'API).
"""
import dagster as dg

from .airbyte_sync import RAW_SALES_KEY
from .db import mysql_scalar, pg_scalar, record_check


def _finish(context: dg.AssetCheckExecutionContext, name: str, passed: bool, md: dict):
    record_check(context.run.run_id, name, passed, md)
    return dg.AssetCheckResult(passed=passed, metadata=md)


@dg.asset_check(asset=RAW_SALES_KEY, blocking=True, description="Lignes source == lignes destination, et > 0")
def check_row_count(context: dg.AssetCheckExecutionContext):
    src = mysql_scalar("SELECT COUNT(*) FROM sales")
    dst = pg_scalar("SELECT COUNT(*) FROM public.sales")
    return _finish(
        context, "row_count", dst > 0 and src == dst,
        {"source_rows": src, "destination_rows": dst},
    )


@dg.asset_check(asset=RAW_SALES_KEY, blocking=True, description="Aucun identifiant en double ou nul")
def check_unique_ids(context: dg.AssetCheckExecutionContext):
    dup = pg_scalar("SELECT COUNT(*) - COUNT(DISTINCT id) FROM public.sales")
    nul = pg_scalar("SELECT COUNT(*) FROM public.sales WHERE id IS NULL")
    return _finish(
        context, "unique_ids", dup == 0 and nul == 0,
        {"duplicate_ids": dup, "null_ids": nul},
    )


@dg.asset_check(asset=RAW_SALES_KEY, blocking=True, description="Montants non nuls et strictement positifs")
def check_valid_amounts(context: dg.AssetCheckExecutionContext):
    bad = pg_scalar("SELECT COUNT(*) FROM public.sales WHERE amount IS NULL OR amount <= 0")
    return _finish(context, "valid_amounts", bad == 0, {"invalid_amounts": bad})


all_checks = [check_row_count, check_unique_ids, check_valid_amounts]
