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

## PII / Data Handling

Row-level PII (names, emails, phones, street addresses, gift amounts) **never gets
committed to git** — repos here are org-visible via shared corpora and export pipelines.
Any directory that will receive raw dumps or query results gets gitignored BEFORE the
first file lands (allowlist known-clean file types; never enumerate known-bad files).
Committed derivatives must be masked or aggregated; fabricate example rows in docs.
Row-level people-data lives in access-controlled systems (BigQuery, ROI, Action Network,
shared Sheets) — point at it, don't copy it. Full policy: knowledge library entry
`pii-handling-policy` (`kl_get`).

## Key Files
- `sync.py` — Main sync script. Reads config, fetches Airtable records, loads to BigQuery.
  Uses Airtable metadata API to get column structure so empty tables get correct schemas.
- `config.yaml` — Top-level config: BQ project/dataset + list of sync file paths.
- `syncs/*.yaml` — One per Airtable base. Lists tables to sync with bq_table names.
- `syncs/million_conversations.yaml` — 1 Million Conversations base (appPuybhyk2FskqMG).
- `civis_run.sh` — Two-line shell script for Civis container jobs (installs deps + runs sync).
  Lives at repo root (predates the `civis/*.sh` convention).
- `civis/SCHEDULED_SCRIPTS.md` — Machine-parsed Civis job manifest (schedule, APIs, credentials);
  pulled into the meta-project's cloud schedule rollup. Keep it current when the job changes.
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
- Schedule: daily at 3:00 AM ET
- Job command is `bash app/civis_run.sh` (GitHub-backed; Civis clones the repo into `app/`)
- See `civis/SCHEDULED_SCRIPTS.md` for the full manifest, `civis_config.md` for the script link

## Architecture Notes
- Full-replace sync: every run truncates and reloads each BQ table
- Column names are sanitized from Airtable field names to snake_case
- Metadata columns added: `_airtable_record_id`, `_synced_at`
- List/dict Airtable values are JSON-serialized to strings
- Columns are cast to native BQ types using Airtable field metadata (`AIRTABLE_TYPE_MAP` in
  `sync.py`): number/currency/percent/duration → Float64, autoNumber/count/rating → Int64,
  checkbox → boolean, date/dateTime/createdTime/lastModifiedTime → TIMESTAMP; unmapped types
  stay strings. Failed casts warn and leave the column as-is.
- Empty Airtable tables produce empty BQ tables with correct column structure
- BQ views referencing these tables survive syncs; new columns appear in `SELECT *` views
- Target: `proj-tmc-mem-com.million_conversations`
