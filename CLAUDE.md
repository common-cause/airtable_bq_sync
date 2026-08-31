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

## Agent Automation & Dispatch

Two different mechanisms. Picking the wrong one wastes the build:

- **Deterministic pipeline → Civis.** Plain Python/dbt ETL, no judgment; tracked in
  this project's `civis/SCHEDULED_SCRIPTS.md`.
- **Judgment pass → local scheduled agent, via a dispatch contract.** Anything whose
  correctness depends on a rubric, world knowledge, or a call a human would otherwise
  make. Subscription Claude Code **cannot be invoked from Civis at all** — no API-key
  path there uses the subscription — so "a Civis job that exercises judgment" is
  unbuildable, not merely discouraged. Don't start building one.

Agent-dispatchable work is governed by the **Dispatch Treaty** (ratified 2026-08-20,
in force since 2026-08-25; law: meta-project `docs/dispatch_treaty.md`). The
rob-assistant "tower" spawns headless agents at named task types that a project
declares in a committed contract. Live fleet status — who has declared what, and what
is actually granted — is the meta-project's generated `dispatch/roster.yaml`; don't
trust a count written in prose anywhere, including here.

**To make a task type in this project dispatchable:**

1. Write `.claude/dispatch.yaml` from the meta-project's `templates/dispatch.yaml`
   (one file, all of this project's task types). **Absence of that file means
   hands-off** — eligibility is declared, never inferred, and no stub is wanted for
   an interactive-only project.
2. Package the procedure itself as the runbook the contract points at — a skill at
   `.claude/skills/<name>/SKILL.md`, or a doc under `docs/`.
3. Confirm **git can see the contract.** A blanket `.claude/*` gitignore swallows it
   silently; add `!.claude/dispatch.yaml`. A contract git can't see does not exist.
4. Validate from the meta-project: `python sync_projects.py --check`, then
   `--dispatch-roster`.
5. **Stop there.** Tiers are dated grants that live only in the meta catalog
   (`projects_index.yaml`), and **only Rob grants one** — an agent proposes, never
   self-authorizes. An ungranted contract is the correct resting state: the roster
   computes `dispatchable: false` and nothing fires.

Do not register a Windows Task Scheduler job for an agent pass either — scheduled
fires go through the tower, or they earn no track record. Background and the
scheduler mechanics: knowledge library entries `dispatch-treaty-and-the-tower` and
`local-scheduled-claude-agents-task-scheduler-the-pattern-for-recurring-agentic-p`
(`kl_get`).

## Key Files
- `sync.py` — Main sync script. Reads config, fetches Airtable records, loads to BigQuery.
  Uses Airtable metadata API to get column structure so empty tables get correct schemas.
- `config.yaml` — Top-level config: BQ project/dataset + list of sync file paths.
- `syncs/*.yaml` — One per Airtable base. Lists tables to sync with bq_table names.
- `syncs/million_conversations.yaml` — 1 Million Conversations base (appPuybhyk2FskqMG).
- `civis_run.sh` — Civis container entrypoint: installs `app/requirements.txt` plus
  `ccef-connections[airtable,bigquery]` pinned to a release tag, then runs the sync.
  Keep the pin — the unpinned, extras-less install failed at import for 82 straight
  nights after ccef-connections 0.2.0 moved BigQuery deps behind extras (2026-06-04);
  bump the tag deliberately when upgrading. Lives at repo root (predates the
  `civis/*.sh` convention).
- `.claude/dispatch.yaml` — Dispatch contract. Declares the `assign-event-states` task type
  under the Dispatch Treaty (meta repo `docs/dispatch_treaty.md`). **Currently UNGRANTED**:
  tiers are dated human grants living in the meta catalog, never here, so nothing fires
  until Rob grants it. Validate with `python sync_projects.py --check` from the meta repo.
- `.claude/skills/assign-event-states/SKILL.md` — The runbook, and the source of truth for
  the judgment. Carries the rubric (explicit state in text → virtual marker → venue with no
  state → named national event → unresolvable) and the traps: "Delaware Valley Action! …
  Milford, PA" is PA not DE; uppercase-only 2-letter matching; "March on Washington" is DC.
- `assign_event_states.py` — I/O only, makes **no decisions**. `--list` emits unassigned
  Event Reports as JSON joined to host home states (the detection surface); `--set rec=ST`
  writes, re-reading each row first and refusing any that already has a state.
- `seed_host_states.py` — Local-only. Seeds/refreshes the `Hosts` table (email → state) in
  the base from the KL entry `common-cause-staff-directory-and-org-chart`. Updates rows,
  never deletes, so hand-added hosts survive.
- `civis/SCHEDULED_SCRIPTS.md` — Machine-parsed Civis job manifest (schedule, APIs, credentials);
  pulled into the meta-project's cloud schedule rollup. Keep it current when the job changes.
- `civis_config.md` — Local-only (gitignored) deployment notes with Civis script link and schedule.

## How to Run
```bash
python sync.py                              # sync all tables
python sync.py --only event_reports         # sync one table
python sync.py --allow-shrink               # bypass the row-count floor guard

python assign_event_states.py --list        # unassigned Event Reports + host states (JSON)
python assign_event_states.py --set recX=PA  # write one assignment
python seed_host_states.py --apply          # refresh Hosts from the KL staff directory
```

The event-state pass is an **agent judgment task**, not a script to run: invoke
`/assign-event-states` and follow the runbook. The rule of thumb this project follows is
*deterministic pipeline → Civis; judgment pass → local scheduled agent* — subscription
Claude Code cannot be invoked from Civis at all, so the sync stays on Civis and this pass
does not. See KL `local-scheduled-claude-agents-task-scheduler-the-pattern-for-recurring-agentic-p`.

New Airtable fields need no code change — `sync.py` reads the live Airtable metadata API,
so a column added in the base appears in BigQuery on the next run (`Event State` arrived
this way).

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
- Metadata columns added: `_airtable_record_id`, `_airtable_created_time` (TIMESTAMP),
  `_synced_at`. `_airtable_created_time` comes from Airtable's per-record `createdTime`
  and is typed by declaring it as a `createdTime` field in `fetch_base_schema` — it is
  the only dependable time dimension in this base, since the user-entered date fields
  (`Conversation Date`, `Event Date`) are in practice left blank on every record.
  Any new metadata column must be added to `fetch_base_schema` as well as
  `flatten_record`: `sync_table` reindexes to exactly `list(field_types)` and silently
  drops anything missing from it.
- Row-count floor guard: a load that would drop a table below `SHRINK_GUARD_RATIO`
  (50%) of its current BQ row count raises `ShrinkGuardError` and fails that table
  instead of truncating it. Full-replace means a successful-but-empty Airtable response
  is otherwise indistinguishable from a legitimate delete — both truncate and both exit
  0. Tables that are already empty in BQ are exempt, so genuinely-empty tables like
  `event_reports_attendees` still sync. Use `--allow-shrink` when a large drop is real.
- List/dict Airtable values are JSON-serialized to strings
- Columns are cast to native BQ types using Airtable field metadata (`AIRTABLE_TYPE_MAP` in
  `sync.py`): number/currency/percent/duration → Float64, autoNumber/count/rating → Int64,
  checkbox → boolean, date/dateTime/createdTime/lastModifiedTime → TIMESTAMP; unmapped types
  stay strings. Failed casts warn and leave the column as-is.
- Empty Airtable tables produce empty BQ tables with correct column structure AND types —
  type coercion runs for the empty branch too (BQ autodetect on an all-object empty frame
  would otherwise type every column INT64 and break downstream views)
- BQ views referencing these tables survive syncs; new columns appear in `SELECT *` views
- Target: `proj-tmc-mem-com.million_conversations`
