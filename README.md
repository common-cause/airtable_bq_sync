# Airtable BQ Sync

> Replicates Airtable tables into BigQuery -- config-driven full-replace sync, designed for Civis scheduling

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

Locally, also install the shared connections library:
```bash
pip install -e "C:/Users/RobKerth/OneDrive - Common Cause Education Fund/Documents/AI Interpretation/ccef-connections"
```

## Configuration

**`config.yaml`** -- top-level settings and list of sync files:

```yaml
bigquery:
  project: proj-tmc-mem-com
  dataset: million_conversations

syncs:
  - syncs/million_conversations.yaml
```

**`syncs/*.yaml`** -- one file per Airtable base, listing tables to sync:

```yaml
base_id: appXXXXXXXXXXXXXX

tables:
  - name: Contacts          # Airtable table name
    bq_table: contacts      # destination table in BigQuery
    # view: Grid view       # optional: sync only a specific view
```

## Usage

```bash
# Sync all configured tables
python sync.py

# Sync a single table by its bq_table name
python sync.py --only contacts
```

## How it works

Each run does a **full replace** of every configured BigQuery table:

1. Fetch the Airtable base schema (metadata API) to know all column names and field types
2. Fetch all records from each table (optionally filtered by view)
3. Flatten fields into columns (names sanitized to `snake_case`); list/dict values are JSON-serialized to strings
4. Add `_airtable_record_id` and `_synced_at` metadata columns
5. Cast columns to native BQ types from Airtable field metadata (numbers/currency/percent/duration → FLOAT, autoNumber/count/rating → INT, checkbox → BOOL, date/dateTime → TIMESTAMP); everything else loads as STRING
6. Load into BigQuery with `WRITE_TRUNCATE` (full replace)

If a table has no records, an **empty table with the correct column structure and
types** is created (types come from the Airtable field metadata, not BigQuery
inference) so that downstream views and queries can reference it immediately.

**View stability:** BigQuery views referencing these tables continue to work across
syncs. New Airtable columns appear automatically in `SELECT *` views. Removed columns
will require view updates.

## Civis deployment

Set these as Civis credentials (they follow the `{NAME}_PASSWORD` convention):
- `AIRTABLE_API_KEY_PASSWORD` -- Airtable personal access token
- `BIGQUERY_CREDENTIALS_PASSWORD` -- GCP service account JSON (unquoted)

The live job is a GitHub-backed container script: Civis clones this repo and runs
`bash app/civis_run.sh` (installs `app/requirements.txt` plus
`ccef-connections[airtable,bigquery]` pinned to a release tag, then runs
`python app/sync.py`). Keep the pin — bump the tag deliberately when upgrading.
Full job details (script link, schedule, credentials) live in
`civis/SCHEDULED_SCRIPTS.md`.

## Project structure

```
airtable-bq-sync/
├── config.yaml                       # BQ project/dataset + sync file list
├── syncs/
│   ├── example.yaml                  # Template for new bases
│   └── million_conversations.yaml    # 1M Conversations base
├── sync.py                           # Main sync script
├── civis_run.sh                      # Civis container entrypoint (root-level; predates civis/*.sh convention)
├── civis/
│   └── SCHEDULED_SCRIPTS.md          # Civis job manifest (schedule, APIs, credentials)
├── .env.example                      # Credential template
├── requirements.txt
└── README.md
```
