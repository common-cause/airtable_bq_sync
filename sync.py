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
        {"id": "recXXX", "fields": {"Name": "Alice", "Tags": ["a", "b"]}}

    Returns:
        {"_airtable_record_id": "recXXX", "name": "Alice", "tags": '["a", "b"]'}

    List/dict field values are JSON-serialised to strings so they survive
    BigQuery ingestion without schema complexity.
    """
    import json

    row = {"_airtable_record_id": record["id"]}
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
    Metadata columns (_airtable_record_id, _synced_at) have type None.
    """
    token = CredentialManager().get_airtable_key()
    resp = requests.get(
        f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    schema = {}
    for table in resp.json()["tables"]:
        fields: dict[str, str | None] = {"_airtable_record_id": None}
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

def sync_table(
    airtable: AirtableConnector,
    bq: BigQueryConnector,
    dataset: str,
    base_id: str,
    table_cfg: dict,
    field_types: dict[str, str | None] | None = None,
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
        # Cast columns to native types based on Airtable field metadata
        if field_types:
            df = coerce_column_types(df, field_types)
        # Convert remaining object columns to StringDtype so None → pd.NA (real NULL)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(pd.StringDtype())
        log.info("  %d records -> %s (%d columns)", len(df), destination, len(df.columns))

    bq.load_dataframe(df, destination, if_exists="replace")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Airtable -> BigQuery sync")
    parser.add_argument(
        "--only",
        help="Sync only the table with this bq_table name",
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
                    n = sync_table(airtable, bq, dataset, base_id, table_cfg, field_types)
                    total_rows += n
                    total_tables += 1
                except Exception:
                    log.exception("Failed to sync %s", table_cfg["bq_table"])
                    errors.append(table_cfg["bq_table"])

    log.info("Done — %d tables, %d total rows synced", total_tables, total_rows)
    if errors:
        log.error("Failed tables: %s", ", ".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
