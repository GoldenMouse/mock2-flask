# MarketArm — Databricks App (Flask)

A minimal [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)
built with Flask. It serves a single `index` route that provides full CRUD over
the `spark_catalog.marketarm.tblentry` Delta table, rendered as a single
server-side `index.html` (pure HTML/CSS, no JS framework).

## Structure

```
app.yaml            # Databricks App runtime config (gunicorn command + env)
app.py              # Flask app: index route + SQL warehouse CRUD helpers
templates/
  index.html        # Single-page UI (list + create/edit form), inline CSS
requirements.txt    # Python dependencies
schema.sql          # Source DDL for tblusers and tblentry
```

## How it connects to data

The app queries a **SQL warehouse** via `databricks-sql-connector`.
Authentication is handled automatically by the app's service principal — the
Databricks SDK `Config` picks up the OAuth credentials injected into the App
runtime, so no tokens are stored in code.

The only thing you must configure is which warehouse to use:

1. In the Databricks UI, attach a **SQL warehouse** resource to the app
   (or note an existing warehouse's HTTP path).
2. Set `DATABRICKS_HTTP_PATH` in `app.yaml` to that warehouse's HTTP path,
   e.g. `/sql/1.0/warehouses/abc123def456`.
3. Grant the app's service principal `SELECT`/`MODIFY` on
   `spark_catalog.marketarm.tblentry`.

## Deploy

Using the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html):

```bash
# Upload the source to your workspace
databricks sync . /Workspace/Users/<you>/marketarm-app

# Create the app once
databricks apps create marketarm-app

# Deploy
databricks apps deploy marketarm-app \
  --source-code-path /Workspace/Users/<you>/marketarm-app
```

## Run locally (mock data — no Databricks needed)

The app uses an in-memory **mock backend** (seeded rows in `data.py`) whenever
no warehouse is configured, so you can develop the UI and CRUD flow completely
offline. A yellow **MOCK DATA** badge appears in the header when this is active.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py            # http://localhost:8000
```

Mock data lives in memory only: changes reset each time you restart the
process. Force mock mode even when a warehouse is set with `USE_MOCK=1`.

## Run locally against a real warehouse

Set the three env vars and mock mode turns off automatically:

```powershell
$env:DATABRICKS_HOST      = "https://<workspace>.cloud.databricks.com"
$env:DATABRICKS_TOKEN     = "<personal-access-token>"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
python app.py            # http://localhost:8000
```

## Notes

- `tblentry` has no identity column, so new `entryID` values are allocated as
  `MAX(entryID) + 1`. Fine for a single-user app; add a sequence/UUID strategy
  if you expect concurrent writes.
- `tblusers` is included in `schema.sql` for reference; auth/user management is
  not wired up yet (the app currently exposes one anonymous `index` route).
