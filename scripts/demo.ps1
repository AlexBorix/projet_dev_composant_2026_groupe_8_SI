$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
Set-Location (Split-Path -Parent $PSScriptRoot)

function Invoke-Pipeline {
    param([switch]$ExpectFailure)

    & docker compose exec -T dagster dagster job execute -m dagster_project.definitions -j sales_pipeline
    $exitCode = $LASTEXITCODE
    if ($ExpectFailure) {
        if ($exitCode -eq 0) {
            throw "Le pipeline aurait du echouer avec la vente invalide."
        }
    } elseif ($exitCode -ne 0) {
        throw "Le pipeline a echoue (code $exitCode)."
    }
}

function Get-LatestQuality {
    Invoke-RestMethod -Uri "http://localhost:8080/quality/latest"
}

function Assert-SalesCount {
    param([int]$Expected)
    $result = Invoke-RestMethod -Uri "http://localhost:8080/sales"
    if ($result.count -ne $Expected) {
        throw "Nombre de ventes attendu : $Expected ; obtenu : $($result.count)."
    }
}

Write-Host "=== 1. EXECUTION NORMALE ==="
Invoke-Pipeline
Assert-SalesCount -Expected 5
$quality = Get-LatestQuality
if (-not $quality.all_passed -or $quality.checks.Count -ne 3) {
    throw "Les controles auraient du reussir lors de l'execution normale."
}
Write-Host "OK : 5 ventes et controles valides."

Write-Host "`n=== 2. DONNEE INVALIDE ==="
& docker compose exec -T mysql mysql -uroot -prootpass shop `
    -e "INSERT INTO sales (customer, product, amount) VALUES ('Test', 'Anomalie', -5.00);"
if ($LASTEXITCODE -ne 0) {
    throw "Impossible d'injecter la vente invalide dans MySQL."
}
Invoke-Pipeline -ExpectFailure
$quality = Get-LatestQuality
$amountCheck = $quality.checks | Where-Object { $_.check_name -eq "valid_amounts" } | Select-Object -First 1
if ($quality.all_passed -or $quality.checks.Count -ne 3 -or $null -eq $amountCheck -or $amountCheck.passed) {
    throw "Le controle valid_amounts aurait du echouer."
}
Assert-SalesCount -Expected 5
Write-Host "OK : le controle bloque la publication; l'API conserve les 5 ventes precedentes."

Write-Host "`n=== 3. CORRECTION ET RELANCE ==="
& docker compose exec -T mysql mysql -uroot -prootpass shop `
    -e "DELETE FROM sales WHERE amount <= 0;"
if ($LASTEXITCODE -ne 0) {
    throw "Impossible de supprimer la vente invalide de MySQL."
}
Invoke-Pipeline
$quality = Get-LatestQuality
if (-not $quality.all_passed -or $quality.checks.Count -ne 3) {
    throw "Les controles auraient du reussir apres correction."
}
Assert-SalesCount -Expected 5
Write-Host "OK : les controles repassent et l'API expose les 5 ventes."
