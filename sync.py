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


def fetch_base_schema(base_id: str) -> dict[str, list[str]]:
    """Fetch field names for every table in a base via the Airtable metadata API.

    Returns {table_name: [field_name, ...]} with names already sanitised.
    """
    token = CredentialManager().get_airtable_key()
    resp = requests.get(
        f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    schema = {}
    for table in resp.json()["tables"]:
        cols = ["_airtable_record_id"]
        cols += [sanitize_column_name(f["name"]) for f in table["fields"]]
        cols.append("_synced_at")
        schema[table["name"]] = cols
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
    schema_columns: list[str] | None = None,
) -> int:
    """Sync one Airtable table to BigQuery. Returns row count."""
    at_name = table_cfg["name"]
    bq_table = table_cfg["bq_table"]
    view = table_cfg.get("view")
    destination = f"{dataset}.{bq_table}"

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
                schema_columns = base_schema.get(table_cfg["name"])
                try:
                    n = sync_table(airtable, bq, dataset, base_id, table_cfg, schema_columns)
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
