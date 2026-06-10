# Scheduled Scripts — Airtable BQ Sync

*Last verified: 2026-06-04*

## Job Setup (all scripts)

The Civis job is **GitHub-backed**: the repo `common-cause/airtable_bq_sync`
(branch `main`) is attached, Civis clones it into `app/`, and the job body is
just a stub:

```bash
bash app/civis_run.sh
```

**Non-standard layout note:** this project predates the `civis/*.sh` convention —
the run script lives at the repo root (`civis_run.sh`), not under `civis/`.
It pip-installs `ccef-connections` **unpinned from master** plus `pyairtable`,
then runs `python app/sync.py`. ⚠️ Maintenance flag: per current convention the
install should be pinned to a release tag (e.g.
`"ccef-connections[bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.2.0"`)
so library pushes never change this scheduled job — update `civis_run.sh`
deliberately when upgrading.

## Scripts

### civis_run.sh
- **Type:** Individual (Daily at 3:00 AM ET)
- **Civis job name:** Airtable BQ Sync (container #347402326)
- **APIs:** Airtable API (5 req/sec/base), BigQuery (write)
- **Description:** Config-driven full-replace replication of Airtable tables into BigQuery. For each base/table listed in `config.yaml` → `syncs/million_conversations.yaml` (currently the 1 Million Conversations base `appPuybhyk2FskqMG`), reads all records via pyairtable, sanitizes column names to snake_case, adds `_airtable_record_id` / `_synced_at` metadata columns, JSON-serializes list/dict cells, and loads to `proj-tmc-mem-com.million_conversations` with WRITE_TRUNCATE.

#### Civis configuration

| Field | Value |
|---|---|
| Civis script | https://platform.civisanalytics.com/spa/#/scripts/containers/347402326 |
| Source repo | `common-cause/airtable_bq_sync` |
| Branch | `main` |
| Docker image | `civisanalytics/datascience-python:latest` |
| Command | `bash app/civis_run.sh` |

#### Credentials to attach

- `AIRTABLE_API_KEY` — Airtable PAT in the password field; must have access to
  every base referenced in `config.yaml` syncs; consumed as
  `AIRTABLE_API_KEY_PASSWORD` by ccef-connections.
- `BIGQUERY_CREDENTIALS` — service-account JSON (unquoted) in the password
  field; consumed as `BIGQUERY_CREDENTIALS_PASSWORD`.

## On-Demand Scripts

None. Manual runs are done locally via `python sync.py` (reads `.env`).
