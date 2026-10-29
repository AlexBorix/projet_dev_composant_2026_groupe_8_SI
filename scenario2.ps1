Write-Host '=== 2. DONNEE INVALIDE ==='
docker compose exec -T mysql mysql -uroot -prootpass shop -e ""INSERT INTO sales (customer, product, amount) VALUES ('Test', 'Anomalie', -5.00);""
docker compose exec -T dagster dagster job execute -m dagster_project.definitions -j sales_pipeline 2>&1 | Out-String | Select-String -Pattern 'RUN_FAILURE|RUN_SUCCESS' | Write-Host
