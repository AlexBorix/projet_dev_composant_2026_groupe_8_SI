# Démonstration : Scénario 1 (Exécution normale - Test bout en bout)

L'objectif est de démontrer le **test bout en bout** de la chaîne principale : `MySQL → Airbyte → PostgreSQL (public.sales) → Dagster (checks) → PostgreSQL (public.validated_sales) → API FastAPI`.

## 1. Vérifier que l'environnement est démarré

Vérifier que tous les services sont en fonctionnement.

### PowerShell (Windows)
```powershell
docker compose ps
```

On doit voir les conteneurs suivants en état `Up` (et `healthy` pour `mysql` et `postgres`) :
- `sales_mysql`
- `sales_postgres`
- `sales_dagster`
- `sales_api`

Vérifier également qu'Airbyte est bien démarré via `abctl` :

```bash
abctl local status
```

Résultat attendu : Airbyte accessible sur `http://localhost:8000`

## 2. Vérifier les données sources dans MySQL

Montrer qu'il y a bien 5 ventes dans la base source.

```powershell
docker compose exec -T mysql mysql -uroot -prootpass shop -e "SELECT * FROM sales;"
```

**Résultat attendu :**
| id | customer | product | amount | sold_at |
|---|---|---|---|---|
| 1 | Awa | Clavier | 45.90 | ... |
| 2 | Issa | Souris | 19.99 | ... |
| 3 | Fatou | Ecran | 199.00 | ... |
| 4 | Moussa | Casque | 59.50 | ... |
| 5 | Aicha | Webcam | 39.00 | ... |

## 3. Lancer le pipeline Dagster

Deux possibilités pour lancer l'orchestration. Le plus visuel pour une soutenance est l'UI Dagster.

### Option A : Interface Dagster (recommandée pour la démonstration)
1. Ouvrir [http://localhost:3000](http://localhost:3000)
2. Aller dans `Assets` > `sales_pipeline` ou directement dans `Jobs` > `sales_pipeline`
3. Cliquer sur `Materialize all`
4. Suivre l'exécution dans l'onglet `Runs`

### Option B : Ligne de commande
```powershell
docker compose exec -T dagster dagster job execute -m dagster_project.definitions -j sales_pipeline
```

## 4. Observer l'orchestration en temps réel

Dans l'interface Dagster (onglet `Runs` > run en cours), on doit observer l'ordre suivant :

| Étape | Statut attendu | Explication |
|---|---|---|
| `airbyte_mysql_postgres` / `raw_sales` | `Success` | Synchronisation Airbyte MySQL → PostgreSQL. Déclenchement du job Airbyte puis attente de fin. |
| `raw_sales_check_row_count` | `Success` | Vérifie que le nombre de lignes source == destination et > 0 |
| `raw_sales_check_unique_ids` | `Success` | Vérifie qu'il n'y a aucun `id` nul ou dupliqué |
| `raw_sales_check_valid_amounts` | `Success` | Vérifie qu'aucun montant n'est nul ou <= 0 |
| `validated_sales` | `Success` | Copie des lignes valides vers `public.validated_sales` (ne s'exécute que si tous les checks bloquants passent) |

## 5. Vérifier les résultats dans PostgreSQL

Vérifier que les données ont bien été répliquées et filtrées.

```powershell
# Données synchronisées par Airbyte
docker compose exec -T postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM public.sales;"
```

```powershell
# Données validées par Dagster
docker compose exec -T postgres psql -U postgres -d warehouse -c "SELECT * FROM public.validated_sales ORDER BY id;"
```

**Résultat attendu :**
- `public.sales` : 5 lignes
- `public.validated_sales` : 5 lignes (identiques, car toutes les données sont valides)

## 6. Vérifier l'API REST (preuve finale du bout en bout)

L'API doit exposer les ventes validées ainsi que le résultat des contrôles qualité.

### Via navigateur (Swagger)
Ouvrir [http://localhost:8080/docs](http://localhost:8080/docs) et tester les endpoints suivants :

### Via PowerShell / curl

```powershell
# Santé de l'API
Invoke-RestMethod -Uri "http://localhost:8080/health"
```

```powershell
# Liste des ventes validées
Invoke-RestMethod -Uri "http://localhost:8080/sales"
```

```powershell
# Dernier résultat de contrôle qualité
Invoke-RestMethod -Uri "http://localhost:8080/quality/latest"
```

**Résultat attendu :**

```json
// GET /health
{ "status": "ok" }

// GET /sales
{
  "count": 5,
  "sales": [
    { "id": 1, "customer": "Awa", "product": "Clavier", "amount": 45.9, "sold_at": "..." },
    ...
  ]
}

// GET /quality/latest
{
  "run_id": "...",
  "all_passed": true,
  "checked_at": "...",
  "checks": [
    { "check_name": "row_count", "passed": true, "metadata": { "source_rows": 5, "destination_rows": 5 } },
    { "check_name": "unique_ids", "passed": true, "metadata": { "null_ids": 0, "duplicate_ids": 0 } },
    { "check_name": "valid_amounts", "passed": true, "metadata": { "invalid_amounts": 0 } }
  ]
}
```


