#!/usr/bin/env bash
set -euo pipefail

# Scenario ERREUR : insere une vente avec un montant negatif dans la source MySQL
docker compose exec -T mysql mysql -uroot -prootpass shop \
  -e "INSERT INTO sales (customer, product, amount) VALUES ('Test', 'Anomalie', -5.00);"
echo "Anomalie injectee (montant -5.00)."
