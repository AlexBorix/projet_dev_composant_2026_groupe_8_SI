"""Declenchement de la synchronisation Airbyte.

Deux mecanismes, choisis par la variable SYNC_MECHANISM :

1. dagster_airbyte (defaut, officiel) : AirbyteWorkspace + build_airbyte_assets_definitions.
   Materialiser l'asset declenche un job de sync Airbyte puis attend sa fin (polling
   realise par la bibliotheque : sync_and_poll).
2. rest (secours) : appel direct de l'API publique Airbyte
   POST /jobs {connectionId, jobType: "sync"} puis GET /jobs/{jobId} jusqu'a un etat final.

Dans les deux cas, l'asset Dagster s'appelle `raw_sales`; la table Airbyte est `public.sales`.
"""
import os
import time

import dagster as dg
import requests

RAW_SALES_KEY = dg.AssetKey("raw_sales")

AIRBYTE_HOST = os.getenv("AIRBYTE_HOST", "http://host.docker.internal:8000").rstrip("/")
REST_API = f"{AIRBYTE_HOST}/api/public/v1"
CONFIG_API = f"{AIRBYTE_HOST}/api/v1"
CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID", "")
MECHANISM = os.getenv("SYNC_MECHANISM", "dagster_airbyte")


# ---------------------------------------------------------------- mecanisme 2 : REST
def _token() -> str:
    r = requests.post(
        f"{REST_API}/applications/token",
        json={
            "client_id": os.environ["AIRBYTE_CLIENT_ID"],
            "client_secret": os.environ["AIRBYTE_CLIENT_SECRET"],
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _build_rest_asset() -> dg.AssetsDefinition:
    @dg.asset(key=RAW_SALES_KEY, group_name="ingestion", kinds={"airbyte"})
    def raw_sales(context: dg.AssetExecutionContext):
        if not CONNECTION_ID:
            raise dg.Failure("AIRBYTE_CONNECTION_ID n'est pas configure")

        headers = {"Authorization": f"Bearer {_token()}"}
        r = requests.post(
            f"{REST_API}/jobs",
            headers=headers,
            json={"connectionId": CONNECTION_ID, "jobType": "sync"},
            timeout=15,
        )
        r.raise_for_status()
        job_id = r.json()["jobId"]
        context.log.info(f"Job Airbyte {job_id} lance")
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            response = requests.get(
                f"{REST_API}/jobs/{job_id}", headers=headers, timeout=15
            )
            response.raise_for_status()
            status = response.json().get("status")
            context.log.info(f"Job {job_id} : {status}")
            if status == "succeeded":
                return dg.MaterializeResult(metadata={"airbyte_job_id": job_id})
            if status not in ("pending", "running"):
                raise dg.Failure(
                    f"Synchronisation Airbyte terminee avec le statut "
                    f"{status!r} (job {job_id})"
                )
            time.sleep(5)
        raise dg.Failure(f"Timeout : job Airbyte {job_id} toujours en cours")

    return raw_sales


# ------------------------------------------------- mecanisme 1 : dagster-airbyte
def _build_official_assets():
    from dagster_airbyte import (
        AirbyteWorkspace,
        DagsterAirbyteTranslator,
        build_airbyte_assets_definitions,
    )

    workspace = AirbyteWorkspace(
        rest_api_base_url=REST_API,
        configuration_api_base_url=CONFIG_API,
        workspace_id=os.environ["AIRBYTE_WORKSPACE_ID"],
        client_id=os.environ["AIRBYTE_CLIENT_ID"],
        client_secret=os.environ["AIRBYTE_CLIENT_SECRET"],
    )

    class SalesTranslator(DagsterAirbyteTranslator):
        def get_asset_spec(self, props):
            spec = super().get_asset_spec(props)
            if props.table_name == "sales":
                spec = spec.replace_attributes(key=RAW_SALES_KEY, group_name="ingestion")
            return spec

    assets = build_airbyte_assets_definitions(
        workspace=workspace,
        dagster_airbyte_translator=SalesTranslator(),
        connection_selector_fn=lambda conn: conn.id == CONNECTION_ID,
    )
    return list(assets), {"airbyte": workspace}


def load_sync_assets():
    """Retourne (assets, resources)."""
    if not CONNECTION_ID:
        raise ValueError("AIRBYTE_CONNECTION_ID doit etre renseigne dans .env")
    if MECHANISM == "rest":
        return [_build_rest_asset()], {}
    if MECHANISM == "dagster_airbyte":
        return _build_official_assets()
    raise ValueError(
        f"SYNC_MECHANISM invalide : {MECHANISM!r}. "
        "Valeurs autorisees : 'dagster_airbyte' ou 'rest'."
    )
