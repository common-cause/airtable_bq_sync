"""Airtable -> BigQuery full-replace sync.

Reads config.yaml to discover which Airtable tables to replicate,
fetches all records, and loads them into BigQuery (replacing the
existing table each run).

Usage:
    python sync.py                     # sync all tables in config.yaml
    python sync.py --only my_bq_table  # sync a single table by its bq_table name
"""

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

from ccef_connections import AirtableConnector, BigQueryConnector, CredentialManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"

# A full-replace sync will truncate a table to nothing if Airtable hands back
# zero records, so refuse any load that would drop a table below this fraction
# of the row count BigQuery already holds. Override with --allow-shrink.
SHRINK_GUARD_RATIO = 0.5


class ShrinkGuardError(RuntimeError):
    """A load would shrink a BQ table past SHRINK_GUARD_RATIO — see check_shrink_guard."""


# ── helpers ──────────────────────────────────────────────────────────

def sanitize_column_name(name: str) -> str:
    """Turn an Airtable field name into a valid BigQuery column name."""
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)   # replace non-alphanumeric
    name = re.sub(r"_+", "_", name)            # collapse runs of underscores
    name = name.strip("_")
    if name and name[0].isdigit():
        name = f"_{name}"
    return name or "_unnamed"


def flatten_record(record: dict) -> dict:
    """Flatten an Airtable record into a flat dict for BigQuery.

    Airtable records look like:
        {"id": "recXXX", "createdTime": "2026-08-28T12:28:00.000Z",
         "fields": {"Name": "Alice", "Tags": ["a", "b"]}}

    Returns:
        {"_airtable_record_id": "recXXX",
         "_airtable_created_time": "2026-08-28T12:28:00.000Z",
         "name": "Alice", "tags": '["a", "b"]'}

    List/dict field values are JSON-serialised to strings so they survive
    BigQuery ingestion without schema complexity.
    """
    import json

    row = {
        "_airtable_record_id": record["id"],
        # Airtable returns createdTime on every record. It is the only dependable
        # time dimension these tables have — the user-entered date fields
        # (Conversation Date, Event Date) are routinely left blank, which left
        # BigQuery with no way to answer "how many came in last week" at all.
        "_airtable_created_time": record.get("createdTime"),
    }
    for field_name, value in record.get("fields", {}).items():
        col = sanitize_column_name(field_name)
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        row[col] = value
    return row


# ── Airtable type → pandas dtype mapping ──────────────────────────────
# Airtable field types that map to native BQ types via pandas nullable dtypes.
# Types not listed here stay as object (string) — the safe default.

AIRTABLE_TYPE_MAP: dict[str, str] = {
    # Numeric
    "number": "Float64",
    "currency": "Float64",
    "percent": "Float64",
    "duration": "Float64",       # seconds
    "autoNumber": "Int64",
    "count": "Int64",
    "rating": "Int64",
    # Boolean
    "checkbox": "boolean",
    # Temporal — handled specially in coerce_column_types (not a simple astype)
    "date": "_datetime",
    "dateTime": "_datetime",
    "createdTime": "_datetime",
    "lastModifiedTime": "_datetime",
}


def coerce_column_types(
    df: pd.DataFrame,
    field_types: dict[str, str | None],
) -> pd.DataFrame:
    """Cast DataFrame columns to appropriate pandas types based on Airtable field types.

    Operates in-place for efficiency but also returns the DataFrame.
    Columns that fail to cast are left as-is with a warning.
    """
    for col, at_type in field_types.items():
        if col not in df.columns or at_type is None:
            continue
        target = AIRTABLE_TYPE_MAP.get(at_type)
        if target is None:
            continue
        try:
            if target == "_datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            else:
                df[col] = df[col].astype(target)
        except Exception:
            log.warning("  Could not cast column %s (AT type %s) to %s — leaving as-is", col, at_type, target)
    return df


def fetch_base_schema(base_id: str) -> dict[str, dict[str, str | None]]:
    """Fetch field names and types for every table in a base via the Airtable metadata API.

    Returns {table_name: {sanitised_col_name: airtable_type, ...}}.
    Metadata columns (_airtable_record_id, _synced_at) have type None;
    _airtable_created_time is declared as Airtable's own createdTime type so it
    is cast to TIMESTAMP like any other temporal field.
    """
    token = CredentialManager().get_airtable_key()
    resp = requests.get(
        f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    schema = {}
    for table in resp.json()["tables"]:
        # These must be present here, not just in flatten_record: sync_table
        # reindexes the frame to exactly list(field_types), so a column missing
        # from this dict is silently dropped before the load.
        fields: dict[str, str | None] = {
            "_airtable_record_id": None,
            "_airtable_created_time": "createdTime",
        }
        for f in table["fields"]:
            fields[sanitize_column_name(f["name"])] = f.get("type")
        fields["_synced_at"] = None
        schema[table["name"]] = fields
    return schema


def load_config() -> dict:
    """Read the top-level config.yaml and merge in all referenced sync files."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    sync_files = config.get("syncs", [])
    bases = []
    for rel_path in sync_files:
        full_path = CONFIG_PATH.parent / rel_path
        with open(full_path) as f:
            bases.append(yaml.safe_load(f))
    config["_bases"] = bases
    return config


# ── core sync ────────────────────────────────────────────────────────

def check_shrink_guard(bq: BigQueryConnector, destination: str, new_count: int) -> None:
    """Refuse a load that would drop `destination` below SHRINK_GUARD_RATIO of its rows.

    Every run replaces each table outright, so a successful-but-empty Airtable
    response is indistinguishable from a table that was legitimately emptied:
    both truncate BigQuery, and both report success. A hard API error is already
    safe — it raises, and the existing table is left untouched — so this covers
    the quiet case, where the sync cheerfully deletes real data and exits 0.

    Tables that are genuinely empty stay allowed: the guard only fires when
    BigQuery currently holds rows that the incoming frame would remove.
    """
    if not bq.table_exists(destination):
        return
    existing = next(iter(bq.query(f"SELECT COUNT(*) AS n FROM `{destination}`")))["n"]
    if existing == 0 or new_count >= existing * SHRINK_GUARD_RATIO:
        return
    raise ShrinkGuardError(
        f"Refusing to load {destination}: {new_count} incoming rows vs {existing} "
        f"already in BigQuery, below the {SHRINK_GUARD_RATIO:.0%} floor. Airtable may "
        f"have returned an incomplete result. If the drop is real, re-run with "
        f"--allow-shrink."
    )


def sync_table(
    airtable: AirtableConnector,
    bq: BigQueryConnector,
    dataset: str,
    base_id: str,
    table_cfg: dict,
    field_types: dict[str, str | None] | None = None,
    allow_shrink: bool = False,
) -> int:
    """Sync one Airtable table to BigQuery. Returns row count."""
    at_name = table_cfg["name"]
    bq_table = table_cfg["bq_table"]
    view = table_cfg.get("view")
    destination = f"{dataset}.{bq_table}"
    schema_columns = list(field_types) if field_types else None

    log.info("Fetching %s.%s%s", base_id, at_name, f" (view: {view})" if view else "")

    kwargs = {"base_id": base_id, "table_name": at_name}
    if view:
        kwargs["view"] = view
    records = airtable.get_records(**kwargs)

    if not records:
        if schema_columns:
            log.info("  No records — creating empty table %s (%d columns)", destination, len(schema_columns))
            df = pd.DataFrame(columns=schema_columns)
        else:
            log.warning("  No records and no schema — skipping %s", destination)
            return 0
    else:
        synced_at = datetime.now(timezone.utc).isoformat()
        rows = []
        for rec in records:
            row = flatten_record(rec)
            row["_synced_at"] = synced_at
            rows.append(row)
        df = pd.DataFrame(rows)
        # Ensure all schema columns are present (covers fields with no data yet)
        if schema_columns:
            for col in schema_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[schema_columns]
        log.info("  %d records -> %s (%d columns)", len(df), destination, len(df.columns))

    # Typing runs for BOTH branches. An empty frame's columns are all dtype object,
    # which gives BigQuery nothing to infer from — every column then lands as INT64
    # and any downstream view expecting STRING/BOOL fails to parse outright. That is
    # not hypothetical: it broke four 1mc_* views the first time this table synced
    # empty. Cast from the Airtable field metadata instead of letting BQ guess.
    if field_types:
        df = coerce_column_types(df, field_types)
    # Convert remaining object columns to StringDtype so None → pd.NA (real NULL)
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(pd.StringDtype())

    if not allow_shrink:
        check_shrink_guard(bq, destination, len(df))

    bq.load_dataframe(df, destination, if_exists="replace")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Airtable -> BigQuery sync")
    parser.add_argument(
        "--only",
        help="Sync only the table with this bq_table name",
    )
    parser.add_argument(
        "--allow-shrink",
        action="store_true",
        help="Bypass the row-count floor guard (use when records were really deleted)",
    )
    args = parser.parse_args()

    config = load_config()
    dataset = config["bigquery"]["dataset"]
    project = config["bigquery"].get("project")

    total_rows = 0
    total_tables = 0
    errors = []

    with AirtableConnector() as airtable, BigQueryConnector(project_id=project) as bq:
        for base_cfg in config["_bases"]:
            base_id = base_cfg["base_id"]
            log.info("Fetching schema for base %s", base_id)
            base_schema = fetch_base_schema(base_id)
            for table_cfg in base_cfg.get("tables", []):
                if args.only and table_cfg["bq_table"] != args.only:
                    continue
                field_types = base_schema.get(table_cfg["name"])
                try:
                    n = sync_table(
                        airtable, bq, dataset, base_id, table_cfg, field_types,
                        allow_shrink=args.allow_shrink,
                    )
                    total_rows += n
                    total_tables += 1
                except ShrinkGuardError as e:
                    # Actionable on its own — a traceback here just buries the message
                    # in the failure email.
                    log.error("%s", e)
                    errors.append(table_cfg["bq_table"])
                except Exception:
                    log.exception("Failed to sync %s", table_cfg["bq_table"])
                    errors.append(table_cfg["bq_table"])

    log.info("Done — %d tables, %d total rows synced", total_tables, total_rows)
    if errors:
        log.error("Failed tables: %s", ", ".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
