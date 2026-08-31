"""Assign `Event State` on Airtable Event Reports rows that don't have one.

Resolution order, highest confidence first:

  1. An explicit state at a comma boundary in the location text ("Camden, NC").
  2. A virtual marker ("Zoom", "Virtual", "Online") — the text carries no
     geography, so the host's home state stands in.
  3. No geographic signal at all ("First Unitarian Portland") — host state.
  4. A bare state name or code sitting loose in the text — only trusted when the
     host's state agrees, because this tier is where the false positives live.

Anything unresolved is left blank and reported, never guessed.

Host states come from the `Hosts` table in the same base, which is seeded from
the knowledge library's staff directory by `seed_host_states.py`. That seeding
runs locally (the KL lives on a workstation, not in Civis); this job only ever
reads the table, so it runs unattended.

Usage:
    python assign_event_states.py                 # dry run — proposes, writes nothing
    python assign_event_states.py --apply         # write the assignments back
    python assign_event_states.py --fail-on-unresolved   # exit 1 if any row needs a human
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

from ccef_connections import CredentialManager

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"

EVENTS_TABLE = "Event Reports"
HOSTS_TABLE = "Hosts"
STATE_FIELD = "Event State"
LOCATION_FIELD = "Event City"
HOST_FIELD = "Volunteer Email"

API = "https://api.airtable.com/v0"

STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}
STATE_CODES = set(STATE_NAMES.values())

# "Washington" is deliberately excluded from loose matching: on its own it is at
# least as likely to mean DC (or "March on Washington") as the state, and the
# geo-crosswalk work already showed national/Capitol events landing in that gap.
AMBIGUOUS_BARE_NAMES = {"washington"}

VIRTUAL_MARKERS = (
    "zoom", "virtual", "online", "remote", "webinar", "teleconference",
    "google meet", "gmeet", "ms teams", "microsoft teams", "webex",
    "phone call", "phone bank", "by phone", "over the phone", "conference call",
)


# ── location parsing ─────────────────────────────────────────────────

def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def is_virtual(text: str) -> bool:
    """True when the location describes a meeting medium rather than a place."""
    low = _normalise(text).lower()
    return any(m in low for m in VIRTUAL_MARKERS)


def explicit_state(text: str) -> str | None:
    """A state anchored to a comma — the only pattern trustworthy on its own.

    Comma-anchoring is what separates "Milford, PA" (the real location) from the
    "Delaware" sitting inside "Delaware Valley Action!" in the same cell. A loose
    name search on that string returns DE and is wrong.
    """
    text = _normalise(text)
    if not text:
        return None

    # "Washington, D.C." / "Washington DC" before anything else — the comma rule
    # below would otherwise read the "D.C." as a malformed state name.
    if re.search(r"\bwashington,?\s*d\.?\s*c\.?\b", text, re.IGNORECASE):
        return "DC"

    # ", ST" — uppercase only, so the English words "in", "or", "me", "de"
    # can never be mistaken for Indiana, Oregon, Maine or Delaware.
    for m in re.finditer(r",\s*([A-Z]{2})\b", text):
        if m.group(1) in STATE_CODES:
            return m.group(1)

    # ", Full State Name"
    for m in re.finditer(r",\s*([A-Za-z][A-Za-z ]{2,30})", text):
        cand = _normalise(m.group(1)).lower().rstrip(".")
        if cand in STATE_NAMES:
            return STATE_NAMES[cand]
    return None


def bare_state(text: str) -> str | None:
    """A state name loose in the text. Low confidence — must be corroborated."""
    low = _normalise(text).lower()
    for name, code in STATE_NAMES.items():
        if name in AMBIGUOUS_BARE_NAMES:
            continue
        if re.search(rf"\b{re.escape(name)}\b", low):
            return code
    return None


def resolve_state(location: str, host_email: str, host_states: dict) -> tuple[str | None, str]:
    """Return (state_code_or_None, human-readable reason)."""
    host_state = host_states.get((host_email or "").strip().lower())
    host_note = f"host {host_email}" if host_email else "no host on record"

    code = explicit_state(location)
    if code:
        return code, f"explicit state in location text ({code})"

    if is_virtual(location):
        if host_state:
            return host_state, f"virtual event -> {host_note} is {host_state}"
        return None, f"virtual event but {host_note} has no state in Hosts"

    loose = bare_state(location)
    if loose:
        if host_state and loose == host_state:
            return loose, f"location names {loose}, corroborated by {host_note}"
        if host_state:
            return None, (
                f"CONFLICT: location text suggests {loose} but {host_note} "
                f"is {host_state} — needs a human"
            )
        return None, (
            f"location loosely mentions {loose} but nothing corroborates it "
            f"({host_note}) — needs a human"
        )

    if host_state:
        return host_state, f"no geographic signal in location -> {host_note} is {host_state}"
    return None, f"no geographic signal and {host_note} has no state in Hosts"


# ── Airtable I/O ─────────────────────────────────────────────────────

def _headers() -> dict:
    return {"Authorization": f"Bearer {CredentialManager().get_airtable_key()}"}


def base_id() -> str:
    """Read the 1MC base id from the sync config rather than hardcoding it."""
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    sync_path = CONFIG_PATH.parent / config["syncs"][0]
    with open(sync_path) as f:
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


def load_host_states(bid: str) -> dict[str, str]:
    """Email (lowercased) -> state code, from the Hosts table."""
    try:
        records = fetch_all(bid, HOSTS_TABLE)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            log.warning(
                "No `%s` table in the base — every row will fall back to the "
                "location text alone. Run seed_host_states.py to create it.",
                HOSTS_TABLE,
            )
            return {}
        raise
    hosts = {}
    for r in records:
        f = r.get("fields", {})
        email, state = f.get("Email"), f.get("State")
        if email and state:
            hosts[email.strip().lower()] = state.strip().upper()
    log.info("Loaded %d host->state mappings", len(hosts))
    return hosts


def write_states(bid: str, updates: list[tuple[str, str]]) -> None:
    """PATCH Event State onto the given record ids, 10 at a time (Airtable's cap)."""
    url = f"{API}/{bid}/{requests.utils.quote(EVENTS_TABLE, safe='')}"
    for i in range(0, len(updates), 10):
        chunk = updates[i:i + 10]
        resp = requests.patch(
            url,
            headers={**_headers(), "Content-Type": "application/json"},
            json={"records": [
                {"id": rid, "fields": {STATE_FIELD: state}} for rid, state in chunk
            ]},
        )
        resp.raise_for_status()
        log.info("  wrote %d record(s)", len(chunk))


# ── main ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Assign Event State on Event Reports")
    parser.add_argument(
        "--apply", action="store_true",
        help="Write the assignments back to Airtable (default is a dry run)",
    )
    parser.add_argument(
        "--fail-on-unresolved", action="store_true",
        help="Exit 1 if any row still needs a human decision",
    )
    args = parser.parse_args()

    bid = base_id()
    host_states = load_host_states(bid)
    records = fetch_all(bid, EVENTS_TABLE)

    blank = [r for r in records if not (r.get("fields", {}).get(STATE_FIELD) or "").strip()]
    log.info(
        "%d Event Reports, %d already assigned, %d to evaluate",
        len(records), len(records) - len(blank), len(blank),
    )

    resolved: list[tuple[str, str]] = []
    unresolved: list[tuple[str, str, str]] = []

    for r in blank:
        f = r.get("fields", {})
        location = f.get(LOCATION_FIELD) or ""
        state, reason = resolve_state(location, f.get(HOST_FIELD, ""), host_states)
        if state:
            resolved.append((r["id"], state))
            log.info("  %s  %-45r -> %s   (%s)", r["id"], location, state, reason)
        else:
            unresolved.append((r["id"], location, reason))

    if unresolved:
        log.warning("%d row(s) need a human:", len(unresolved))
        for rid, location, reason in unresolved:
            log.warning("  %s  %-45r -> ?   (%s)", rid, location, reason)

    if not resolved:
        log.info("Nothing to write.")
    elif args.apply:
        log.info("Writing %d assignment(s) to Airtable", len(resolved))
        write_states(bid, resolved)
    else:
        log.info("Dry run — %d assignment(s) not written. Re-run with --apply.", len(resolved))

    if unresolved and args.fail_on_unresolved:
        sys.exit(1)


if __name__ == "__main__":
    main()
