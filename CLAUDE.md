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
- `config.yaml` — Top-level config: BQ project/dataset + list of sync file paths.
- `syncs/*.yaml` — One per Airtable base. Lists tables to sync with bq_table names.
- `syncs/million_conversations.yaml` — 1 Million Conversations base (appPuybhyk2FskqMG).

## How to Run
```bash
python sync.py                              # sync all tables
python sync.py --only event_reports         # sync one table
```

In Civis: set `AIRTABLE_API_KEY_PASSWORD` and `BIGQUERY_CREDENTIALS_PASSWORD` as
credentials, then run `python sync.py` as a Python/Docker script.
