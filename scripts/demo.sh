#!/usr/bin/env bash
set -euo pipefail

# Deroule les 3 scenarios et affiche l'API apres chaque etape.
cd "$(dirname "$0")/.."

assert_quality() {
  local expected="$1"
  curl -fsS localhost:8080/quality/latest | python3 -c '
import json, sys
result = json.load(sys.stdin)
expected = sys.argv[1] == "true"
assert result["all_passed"] is expected, result
assert len(result["checks"]) == 3, result
if not expected:
    check = next((item for item in result["checks"] if item["check_name"] == "valid_amounts"), None)
    assert check is not None and check["passed"] is False, result
' "$expected"
}

assert_sales_count() {
  local expected="$1"
  curl -fsS localhost:8080/sales | python3 -c '
import json, sys
result = json.load(sys.stdin)
expected = int(sys.argv[1])
assert result["count"] == expected, result
' "$expected"
}

echo "=== 1. EXECUTION NORMALE ==="
./scripts/run_pipeline.sh
assert_quality true
assert_sales_count 5
echo "OK : 5 ventes, trois controles valides."

echo
echo "=== 2. ERREUR ==="
./scripts/inject_error.sh
if ./scripts/run_pipeline.sh; then
  echo "ERREUR : le pipeline aurait du echouer." >&2
  exit 1
fi
assert_quality false
assert_sales_count 5
echo "OK : le montant invalide bloque la publication."

echo
echo "=== 3. RELANCE APRES CORRECTION ==="
./scripts/fix_error.sh
./scripts/run_pipeline.sh
assert_quality true
assert_sales_count 5
echo "OK : la relance republie les 5 ventes."
