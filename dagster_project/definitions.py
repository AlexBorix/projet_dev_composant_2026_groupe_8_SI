import dagster as dg

from .airbyte_sync import RAW_SALES_KEY, load_sync_assets
from .assets import validated_sales
from .checks import all_checks

sync_assets, sync_resources = load_sync_assets()

# Ordre : sync Airbyte -> checks (bloquants) -> validated_sales
sales_pipeline = dg.define_asset_job(
    name="sales_pipeline",
    selection=dg.AssetSelection.assets(RAW_SALES_KEY, validated_sales.key)
    | dg.AssetSelection.checks_for_assets(RAW_SALES_KEY),
)

hourly = dg.ScheduleDefinition(
    job=sales_pipeline,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[*sync_assets, validated_sales],
    asset_checks=all_checks,
    jobs=[sales_pipeline],
    schedules=[hourly],
    resources=sync_resources,
)
