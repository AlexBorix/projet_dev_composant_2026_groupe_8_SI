#!/usr/bin/env bash
set -euo pipefail

# Scenario RELANCE : corrige la donnee en supprimant la ligne invalide
docker compose exec -T mysql mysql -uroot -prootpass shop \
  -e "DELETE FROM sales WHERE amount <= 0;"
echo "Donnee corrigee."
