# Airtable BQ Sync

Replicates Airtable tables into BigQuery — config-driven full-replace sync, designed for Civis scheduling

## Project Type
bigquery

## Connections & External APIs

**All external API connections use `ccef-connections`.** Do not write your own BigQuery
clients, Action Network clients, or other API clients in individual projects.

The shared library lives at:
```
C:/Users/RobKerth/OneDrive - Common Cause Education Fund/Documents/AI Interpretation/ccef-connections
```
Install it with:
```bash
pip install -e "C:/Users/RobKerth/OneDrive - Common Cause Education Fund/Documents/AI Interpretation/ccef-connections"
```

**If this project needs a connection type not yet in `ccef-connections`:**
Spec it out and build it *in `ccef-connections`*, then use it here.
Do not duplicate connection logic in individual projects — that's exactly what the shared library is for.

## Credential Pattern
All credentials follow `{CREDENTIAL_NAME}_PASSWORD` in `.env` (Civis-compatible).
JSON credentials are stored as unquoted JSON strings. Never commit `.env`.

## Key Files
- `sync.py` — Main sync script. Reads config, fetches Airtable records, loads to BigQuery.
  Uses Airtable metadata API to get column structure so empty tables get correct schemas.
- `config.yaml` — Top-level config: BQ project/dataset + list of sync file paths.
- `syncs/*.yaml` — One per Airtable base. Lists tables to sync with bq_table names.
- `syncs/million_conversations.yaml` — 1 Million Conversations base (appPuybhyk2FskqMG).
- `civis_run.sh` — Two-line shell script for Civis container jobs (installs deps + runs sync).
- `civis_config.md` — Local-only (gitignored) deployment notes with Civis script link and schedule.

## How to Run
```bash
python sync.py                              # sync all tables
python sync.py --only event_reports         # sync one table
```

## Civis Deployment
- Repo: `common-cause/airtable_bq_sync`, branch `main`
- Docker image: `civisanalytics/datascience-python:latest`
- Credentials: `AIRTABLE_API_KEY`, `BIGQUERY_CREDENTIALS`
- Schedule: daily at 3 AM
- See `civis_config.md` for the script link

## Architecture Notes
- Full-replace sync: every run truncates and reloads each BQ table
- Column names are sanitized from Airtable field names to snake_case
- Metadata columns added: `_airtable_record_id`, `_synced_at`
- List/dict Airtable values are JSON-serialized to strings
- Empty Airtable tables produce empty BQ tables with correct column structure
- BQ views referencing these tables survive syncs; new columns appear in `SELECT *` views
- Target: `proj-tmc-mem-com.million_conversations`
