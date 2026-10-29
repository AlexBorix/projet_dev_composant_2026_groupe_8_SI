# Exercice 13 — Orchestration d'un pipeline Airbyte avec Dagster

```
MySQL (shop.sales) --Airbyte--> PostgreSQL (public.sales)
                                     |
   Dagster :  raw_sales (sync) -> 3 checks bloquants -> validated_sales
                                     |
                    API FastAPI : /sales , /quality/latest
```

## 1. Versions utilisées (épinglées dans `requirements.txt`)

| Composant | Version |
|---|---|
| Python (image Docker) | 3.11 |
| dagster / dagster-webserver | 1.13.27 |
| dagster-airbyte | 0.29.27 |
| Airbyte | 2.4.0 (abctl v0.30.4) |
| MySQL / PostgreSQL | 8.0 / 16 |
| FastAPI / uvicorn | 0.162.0 / 0.54.0 |

## 2. Mécanisme de déclenchement d'Airbyte (à documenter, exigé par la consigne)

**Mécanisme principal : intégration officielle `dagster-airbyte`** (`airbyte_sync.py`).

- Ressource `AirbyteWorkspace` (Airbyte auto-hébergé ; `AirbyteCloudWorkspace` est pour Airbyte Cloud). L'ancien `AirbyteResource` / `load_assets_from_airbyte_instance` est le mécanisme *legacy* et n'est pas utilisé.
- `build_airbyte_assets_definitions(workspace, connection_selector_fn=...)` charge la connexion `AIRBYTE_CONNECTION_ID` comme assets Dagster. **Matérialiser l'asset déclenche un job de synchronisation Airbyte puis attend sa fin** (polling interne de la bibliothèque, `sync_and_poll`). Un job Airbyte en échec fait échouer l'asset, donc le run.
- Un `DagsterAirbyteTranslator` renomme l'asset du stream `sales` en `raw_sales`.
- Authentification : `client_id` / `client_secret` (Airbyte > Settings > Applications), jamais en dur dans le code (fichier `.env`).
- URLs : API publique `…/api/public/v1`, API de configuration `…/api/v1` (les deux sont requises par `AirbyteWorkspace`).
- La connexion Airbyte est en planification **Manual** : seul Dagster déclenche les syncs.
- Dans la connexion, choisir le namespace **Destination-defined** afin que le stream utilise le schéma configuré sur la destination. Le pipeline lit `public.sales`.

**Mécanisme de secours : `SYNC_MECHANISM=rest`** — appel direct de l'API publique :
`POST /applications/token` → `POST /jobs {connectionId, jobType:"sync"}` → `GET /jobs/{jobId}` toutes les 5 s jusqu'à un état final (timeout 15 min). Seul `succeeded` valide l'asset ; tout autre état final fait échouer le run.
Utile si la bibliothèque ne correspond pas à votre version d'Airbyte. Avantage : Dagster démarre même si Airbyte est éteint (le mécanisme officiel interroge Airbyte au chargement du code).

**Vérification effectuée** : voir la section 6 (ce qui a été testé et ce qui doit l'être sur votre machine).

## 3. Mise en route

Le mode par défaut `dagster_airbyte` interroge Airbyte lors du chargement des définitions Dagster. Il faut donc installer Airbyte, créer la connexion et renseigner `.env` **avant de démarrer Dagster**.

Sous Windows PowerShell, créer le fichier d'environnement et démarrer d'abord les deux bases :

```powershell
Copy-Item .env.example .env
docker compose up -d mysql postgres
```

### Installer Airbyte (hors docker-compose)

Les commandes `abctl` ci-dessous utilisent Bash et Docker. Sous Windows, exécutez-les dans WSL 2 avec Docker Desktop intégré à WSL, ou dans Git Bash si votre installation Docker est accessible.

```bash
curl -LsfS https://get.airbyte.com | bash -     # installe abctl
abctl local install                              # UI : http://localhost:8000
abctl local credentials                          # mot de passe + client id / secret
```
Dans l'UI, noter le **workspace id** (dans l'URL `/workspaces/<id>/…`) et renseigner `.env`. Remplacer les identifiants Airbyte de `.env.example` et noter la version exacte renvoyée par `abctl version` pour le compte rendu.

### Configurer la connexion (UI Airbyte)
Airbyte tourne dans un cluster `kind` : pour joindre MySQL/PostgreSQL publiés sur votre machine, utiliser `host.docker.internal` (sous Linux : l'IP de l'hôte, ex. `172.17.0.1`).

1. **Source MySQL** : host `host.docker.internal`, port `3307`, base `shop`, user `airbyte`, mot de passe `airbytepass`, SSL désactivé, méthode de réplication *Standard*. Docker publie MySQL du projet sur `3307` côté hôte pour éviter un conflit avec un MySQL local qui utilise déjà `3306`.
2. **Destination PostgreSQL** : host `host.docker.internal`, port `5432`, base `warehouse`, user `postgres` / `postgrespass`, schéma par défaut `public`, SSL `disable`.
3. **Connexion** : stream `sales`, mode **Full refresh | Overwrite**, planification **Manual**.
4. Copier l'**ID de connexion** (URL `/connections/<id>/…`) dans `.env` (`AIRBYTE_CONNECTION_ID`).

> Doublons : pour tester `check_unique_ids`, choisir *Incremental | Append* au lieu d'*Overwrite*, puis synchroniser deux fois après modification d'une ligne.

Après avoir configuré la connexion Airbyte et `.env`, démarrer Dagster et l'API :

```powershell
docker compose up -d --build dagster api
```

Le projet peut aussi démarrer sans Airbyte en mettant `SYNC_MECHANISM=rest` dans `.env`. Ce mode différé ne contacte Airbyte qu'au lancement d'un run ; il ne remplace pas la configuration de la connexion.

## 4. Pipeline Dagster

UI : http://localhost:3000 → job **`sales_pipeline`** → *Materialize all*.

| Étape | Rôle |
|---|---|
| `raw_sales` | Sync Airbyte MySQL → `public.sales` |
| `row_count` (check) | lignes source MySQL == lignes destination, et > 0 |
| `unique_ids` (check) | aucun `id` dupliqué ni nul |
| `valid_amounts` (check) | aucun montant NULL ou ≤ 0 |
| `validated_sales` | copie des lignes valides vers `public.validated_sales` |

**Ordre garanti** : les checks sont attachés à `raw_sales` avec `blocking=True`. Ils s'exécutent après le chargement ; si l'un échoue (sévérité ERROR), le run échoue et `validated_sales` **n'est pas exécuté**. Chaque check journalise son résultat dans `public.quality_results`. Un planificateur horaire (`hourly`) est défini, arrêté par défaut.

## 5. API (http://localhost:8080/docs)

- `GET /sales` — ventes validées ; `GET /sales/{id}`
- `GET /quality/latest` — résultat du dernier run ayant enregistré les trois contrôles (`all_passed` + détail par check)
- `GET /health`

## 6. Tests des 3 scénarios

Automatique sous Windows PowerShell : `.\scripts\demo.ps1`. Sous Bash/WSL/Git Bash : `./scripts/demo.sh`. Les scripts vérifient les résultats des trois scénarios et s'arrêtent si un résultat inattendu est observé. Manuel (pour les captures d'écran) :

1. **Normale** : *Materialize all* → 5 étapes vertes ; `/sales` renvoie 5 ventes ; `/quality/latest` → `all_passed: true`.
2. **Erreur** : `./scripts/inject_error.sh` (montant −5 dans MySQL), relancer → `valid_amounts` échoue, `validated_sales` ignoré, `/quality/latest` → `all_passed: false`. Variante : `abctl local uninstall` / arrêter Airbyte → l'asset `raw_sales` échoue, aucun check ne s'exécute.
3. **Relance** : `./scripts/fix_error.sh`, puis relancer le job complet `sales_pipeline` → Airbyte recharge les données corrigées, les contrôles repassent et l'API renvoie le nouveau résultat. Ne pas utiliser *Re-execute from failure* ici : Dagster pourrait reprendre après l'asset Airbyte et contrôler encore les anciennes données présentes dans PostgreSQL.

### Niveau de vérification
- Les versions Dagster, dagster-airbyte, FastAPI et Uvicorn épinglées dans `requirements.txt` existent sur PyPI. `dagster-airbyte==0.29.27` déclare une dépendance exacte sur `dagster==1.13.27` et prend en charge Python 3.10 à 3.14.
- **Testé localement avec des mocks** : chargement/validation des définitions Dagster, compatibilité des signatures de l'intégration Airbyte, exécution simulée du pipeline normal → contrôle bloquant en échec → relance, ainsi que la logique des réponses API. La configuration Docker Compose et les syntaxes des scripts Bash/PowerShell ont aussi été vérifiées.
- **À valider dans l'environnement de démonstration** : démarrage des conteneurs, synchronisation Airbyte réelle et exécution des trois scénarios. Ils n'ont pas pu être lancés ici, car le moteur Docker n'était pas démarré. Relever la version effective d'Airbyte et conserver les résultats/captures des scénarios pour le rendu.

## 7. Dépannage
- *Dagster ne charge pas le code* : Airbyte injoignable ou identifiants/workspace id/connection id faux → logs `docker compose logs dagster` ; passer temporairement à `SYNC_MECHANISM=rest`.
- *`relation public.sales does not exist`* : la connexion Airbyte n'a pas terminé une synchronisation réussie ou son schéma de destination n'est pas `public`.
