#!/usr/bin/env bash
set -euo pipefail

# Lance le job sales_pipeline dans le conteneur Dagster (alternative a l'UI)
docker compose exec -T dagster dagster job execute -m dagster_project.definitions -j sales_pipeline
