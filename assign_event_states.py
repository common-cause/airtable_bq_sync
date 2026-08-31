"""I/O for the assign-event-states judgment pass. Makes no decisions.

This is deliberately a dumb tool. Deciding which state an Event Report belongs
to is the agent's job, and the rubric lives in the runbook
(.claude/skills/assign-event-states/SKILL.md) — not in here as a regex. An
earlier draft of this file DID carry a parser; it reproduced all six existing
rows and would still have been the wrong shape, because the cases that actually
need deciding ("First Unitarian Portland", "March on Washington", a venue whose
city an agent simply knows) are the ones a pattern cannot reach.

So this exposes exactly two operations:

    python assign_event_states.py --list
        Every Event Report with no Event State, as JSON, joined to the host's
        home state from the Hosts table. This is the detection surface.

    python assign_event_states.py --set recXXX=PA --set recYYY=NC
        Write those assignments. Refuses any record whose Event State is already
        non-blank, so a human's assignment cannot be overwritten even by mistake.

Both are read-modify-write safe to re-run: --set re-reads each row immediately
before writing and skips anything already filled.
"""

import argparse
import json
import sys
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

from ccef_connections import CredentialManager

load_dotenv()

CONFIG_PATH = Path(__file__).parent / "config.yaml"
API = "https://api.airtable.com/v0"

EVENTS_TABLE = "Event Reports"
HOSTS_TABLE = "Hosts"
STATE_FIELD = "Event State"
LOCATION_FIELD = "Event City"
HOST_FIELD = "Volunteer Email"


def _headers() -> dict:
    return {"Authorization": f"Bearer {CredentialManager().get_airtable_key()}"}


def base_id() -> str:
    """Read the 1MC base id from the sync config rather than hardcoding it."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    with open(CONFIG_PATH.parent / config["syncs"][0]) as f:
        return yaml.safe_load(f)["base_id"]


def fetch_all(bid: str, table: str) -> list[dict]:
    records, offset = [], None
    url = f"{API}/{bid}/{requests.utils.quote(table, safe='')}"
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        resp = requests.get(url, headers=_headers(), params=params)
        resp.raise_for_status()
        payload = resp.json()
        records.extend(payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            return records


def load_hosts(bid: str) -> dict[str, dict]:
    """Email (lowercased) -> {name, state}. Empty dict if the table is absent."""
    try:
        records = fetch_all(bid, HOSTS_TABLE)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return {}
        raise
    hosts = {}
    for r in records:
        f = r.get("fields", {})
        if f.get("Email"):
            hosts[f["Email"].strip().lower()] = {
                "name": f.get("Name"),
                "state": (f.get("State") or "").strip().upper() or None,
            }
    return hosts


def unassigned(bid: str) -> list[dict]:
    """The detection surface: rows with no Event State, plus host context."""
    hosts = load_hosts(bid)
    rows = []
    for r in fetch_all(bid, EVENTS_TABLE):
        f = r.get("fields", {})
        if (f.get(STATE_FIELD) or "").strip():
            continue
        host_email = (f.get(HOST_FIELD) or "").strip()
        host = hosts.get(host_email.lower(), {})
        rows.append({
            "record_id": r["id"],
            "location": f.get(LOCATION_FIELD),
            "event_name": f.get("Event Name"),
            "attendee_count": f.get("Attendee Count"),
            "host_name": f.get("Volunteer Name"),
            "host_email": host_email or None,
            "host_home_state": host.get("state"),
            "host_in_hosts_table": host_email.lower() in hosts,
        })
    return rows


def apply_assignments(bid: str, pairs: list[tuple[str, str]]) -> dict:
    """Write state codes, skipping any row that already has one."""
    current = {r["id"]: r.get("fields", {}) for r in fetch_all(bid, EVENTS_TABLE)}
    to_write, skipped = [], []

    for rid, state in pairs:
        if rid not in current:
            skipped.append({"record_id": rid, "reason": "no such record"})
            continue
        existing = (current[rid].get(STATE_FIELD) or "").strip()
        if existing:
            skipped.append({
                "record_id": rid,
                "reason": f"already assigned {existing} — refusing to overwrite",
            })
            continue
        state = state.strip().upper()
        if len(state) != 2 or not state.isalpha():
            skipped.append({"record_id": rid, "reason": f"not a 2-letter code: {state!r}"})
            continue
        to_write.append({"id": rid, "fields": {STATE_FIELD: state}})

    url = f"{API}/{bid}/{requests.utils.quote(EVENTS_TABLE, safe='')}"
    for i in range(0, len(to_write), 10):
        resp = requests.patch(
            url,
            headers={**_headers(), "Content-Type": "application/json"},
            json={"records": to_write[i:i + 10]},
        )
        resp.raise_for_status()

    return {
        "written": [{"record_id": w["id"], "state": w["fields"][STATE_FIELD]} for w in to_write],
        "skipped": skipped,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--list", action="store_true",
                        help="Emit unassigned Event Reports as JSON")
    parser.add_argument("--set", action="append", default=[], metavar="recID=ST",
                        help="Assign a state code; repeatable")
    args = parser.parse_args()

    if not args.list and not args.set:
        parser.error("nothing to do — pass --list or --set")

    bid = base_id()

    if args.list:
        print(json.dumps(unassigned(bid), indent=2))

    if args.set:
        pairs = []
        for item in args.set:
            if "=" not in item:
                parser.error(f"--set expects recID=ST, got {item!r}")
            rid, _, state = item.partition("=")
            pairs.append((rid.strip(), state))
        result = apply_assignments(bid, pairs)
        print(json.dumps(result, indent=2))
        if result["skipped"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
