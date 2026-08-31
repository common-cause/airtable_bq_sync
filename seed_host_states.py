"""Seed / refresh the `Hosts` table in the 1MC base from the KL staff directory.

`assign_event_states.py` falls back to a host's home state whenever the location
text carries no geography ("Zoom", "Virtual", a bare venue name). That mapping
has to come from somewhere; the somewhere is the knowledge library entry
`common-cause-staff-directory-and-org-chart`, which is maintained by agents on
this workstation.

Which is why seeding is a separate, locally-run script: the KL is not reachable
from a Civis container. The scheduled job only reads the Airtable table this
writes, so it stays self-contained.

Run this whenever the staff directory changes:
    python seed_host_states.py            # dry run — shows adds/changes
    python seed_host_states.py --apply    # create the table if needed, then write

Existing rows are updated, never deleted: staff may add hosts by hand (a
volunteer who isn't CC staff, say), and this must not clobber them.

Source: KL `common-cause-staff-directory-and-org-chart`, last_verified 2026-08-18.
Only people the directory gives an actual email for are seeded. The directory
warns that the `firstinitiallastname` pattern breaks (Rosario Palacios is
`mdrpalacios`), so no addresses are inferred here — an unlisted host simply
falls through to "needs a human", which is the safe outcome.
"""

import argparse
import logging
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
API = "https://api.airtable.com/v0"
HOSTS_TABLE = "Hosts"
DOMAIN = "@commoncause.org"

# state -> [(name, email_local_part, source)]
# source: "confirmed" = ✓ in the KL directory; "org chart" = from the directory's
# director table, reliable but not individually re-confirmed.
DIRECTORY: dict[str, list[tuple[str, str, str]]] = {
    "GA": [("Rosario Palacios", "mdrpalacios", "org chart"),
           ("Andres Parra", "aparra", "confirmed"),
           ("Marifer Vizcaino", "mvizcaino", "confirmed"),
           ("Tobias Brown", "tbrown", "confirmed")],
    "AZ": [("Jenny Guzman", "jguzman", "org chart")],
    "CO": [("Aly Belknap", "abelknap", "org chart"),
           ("Jorge Hernandez", "jhernandez", "confirmed")],
    "RI": [("John Marion", "jmarion", "org chart"),
           ("Alicia Vallette", "avallette", "confirmed")],
    "MI": [("Quentin Turner", "qturner", "org chart"),
           ("Shannon Abbott", "sabbott", "confirmed")],
    "MA": [("Geoff Foster", "gfoster", "org chart"),
           ("Dev Chatterjee", "dchatterjee", "confirmed"),
           ("K. Glazer", "kglazer", "confirmed")],
    "MN": [("A Belladonna", "abelladonna", "org chart")],
    "PA": [("Philip Hensley-Robin", "phensleyrobin", "org chart"),
           ("Brett Scruton", "bscruton", "confirmed"),
           ("Jill Greene", "jgreene", "confirmed")],
    "IN": [("Julia Vaughn", "jvaughn", "org chart"),
           ("Layla Ortas", "lortas", "confirmed")],
    "WI": [("Bianca Shaw", "bshaw", "org chart"),
           ("Erin Grunze", "egrunze", "confirmed")],
    "NM": [("Molly Swank", "mswank", "org chart"),
           ("Cesar Marquez", "cmarquez", "confirmed"),
           ("Abraham Sanchez", "asanchez", "confirmed")],
    "MD": [("Joanne Antoine", "jantoine", "org chart"),
           ("Morgan Drayton", "mdrayton", "confirmed")],
    "FL": [("Amy Keith", "akeith", "org chart"),
           ("Franceska Edouard", "fedouard", "confirmed")],
    "CA": [("Darius Kemp", "dkemp", "org chart")],
    "NC": [("Sailor Jones", "jsailorjones", "org chart"),
           ("Rotrina Campbell", "rcampbell", "confirmed"),
           ("Destiny Jordan", "djordan", "confirmed"),
           ("Jazmyne Abney", "jabney", "confirmed"),
           ("Lisette Rodriguez", "lrodriguez", "confirmed"),
           ("Miles Beasley", "mbeasley", "confirmed"),
           ("Shi'Anne Caldwell", "scaldwell", "confirmed"),
           ("Thaddeus Stewart", "tstewart", "confirmed")],
    "NY": [("Susan Lerner", "slerner", "org chart"),
           ("Nia Alvarez-Mapp", "nalvarez-mapp", "confirmed")],
    "HI": [("Camron Hurt", "churt", "org chart")],
    "NE": [("Gavin Geis", "ggeis", "org chart"),
           ("Cheech Sorilla", "csorilla", "confirmed")],
    "TX": [("Anthony Gutierrez", "agutierrez", "org chart"),
           ("Sofia Lozano", "slozano", "confirmed")],
    "OH": [("Catherine Turcer", "cturcer", "org chart"),
           ("Kelly Dufour", "kdufour", "confirmed")],
    "OR": [("Kate Titus", "ktitus", "org chart"),
           ("Frank Stiefel", "fstiefel", "confirmed")],
}

TABLE_SPEC = {
    "name": HOSTS_TABLE,
    "description": (
        "Host -> home state, used by assign_event_states.py when an Event Report's "
        "location has no geography of its own (Zoom, Virtual, a bare venue name). "
        "Seeded from the knowledge library staff directory by seed_host_states.py; "
        "hand-added rows are preserved. Email match is case-insensitive."
    ),
    "fields": [
        {"name": "Name", "type": "singleLineText"},
        {"name": "Email", "type": "email"},
        {"name": "State", "type": "singleLineText"},
        {"name": "Source", "type": "singleLineText"},
        {"name": "Notes", "type": "multilineText"},
    ],
}


def _headers() -> dict:
    return {"Authorization": f"Bearer {CredentialManager().get_airtable_key()}"}


def base_id() -> str:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    with open(CONFIG_PATH.parent / config["syncs"][0]) as f:
        return yaml.safe_load(f)["base_id"]


def table_exists(bid: str) -> bool:
    resp = requests.get(f"{API}/meta/bases/{bid}/tables", headers=_headers())
    resp.raise_for_status()
    return any(t["name"] == HOSTS_TABLE for t in resp.json()["tables"])


def create_table(bid: str) -> None:
    resp = requests.post(
        f"{API}/meta/bases/{bid}/tables",
        headers={**_headers(), "Content-Type": "application/json"},
        json=TABLE_SPEC,
    )
    resp.raise_for_status()
    log.info("Created `%s` table", HOSTS_TABLE)


def fetch_existing(bid: str) -> dict[str, dict]:
    records, offset = [], None
    url = f"{API}/{bid}/{HOSTS_TABLE}"
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
            break
    return {
        r["fields"]["Email"].strip().lower(): r
        for r in records if r.get("fields", {}).get("Email")
    }


def _post(bid: str, rows: list[dict]) -> None:
    for i in range(0, len(rows), 10):
        resp = requests.post(
            f"{API}/{bid}/{HOSTS_TABLE}",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"records": [{"fields": f} for f in rows[i:i + 10]]},
        )
        resp.raise_for_status()


def _patch(bid: str, rows: list[dict]) -> None:
    for i in range(0, len(rows), 10):
        resp = requests.patch(
            f"{API}/{bid}/{HOSTS_TABLE}",
            headers={**_headers(), "Content-Type": "application/json"},
            json={"records": rows[i:i + 10]},
        )
        resp.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the Hosts table from the KL directory")
    parser.add_argument("--apply", action="store_true", help="Actually write to Airtable")
    args = parser.parse_args()

    bid = base_id()
    exists = table_exists(bid)
    if not exists:
        log.info("`%s` table does not exist yet%s", HOSTS_TABLE,
                 "" if args.apply else " — would be created")
        if args.apply:
            create_table(bid)

    existing = fetch_existing(bid) if (exists or args.apply) else {}

    adds, changes = [], []
    for state, people in sorted(DIRECTORY.items()):
        for name, local, source in people:
            email = f"{local}{DOMAIN}"
            current = existing.get(email.lower())
            fields = {
                "Name": name, "Email": email, "State": state,
                "Source": f"KL staff directory ({source})",
            }
            if current is None:
                adds.append(fields)
            elif (current["fields"].get("State") or "").strip().upper() != state:
                changes.append({
                    "id": current["id"],
                    "fields": {"State": state, "Source": fields["Source"]},
                    "_was": current["fields"].get("State"),
                    "_email": email,
                })

    log.info("%d directory entries: %d new, %d state change(s), %d already correct",
             sum(len(v) for v in DIRECTORY.values()), len(adds), len(changes),
             sum(len(v) for v in DIRECTORY.values()) - len(adds) - len(changes))
    for c in changes:
        log.info("  change %s: %s -> %s", c["_email"], c["_was"], c["fields"]["State"])

    untouched = set(existing) - {f"{l}{DOMAIN}".lower()
                                 for v in DIRECTORY.values() for _, l, _ in v}
    if untouched:
        log.info("%d hand-added row(s) left alone: %s", len(untouched), ", ".join(sorted(untouched)))

    if not args.apply:
        log.info("Dry run — nothing written. Re-run with --apply.")
        return

    if adds:
        _post(bid, adds)
        log.info("Added %d row(s)", len(adds))
    if changes:
        _patch(bid, [{"id": c["id"], "fields": c["fields"]} for c in changes])
        log.info("Updated %d row(s)", len(changes))
    if not adds and not changes:
        log.info("Nothing to write — Hosts already matches the directory.")


if __name__ == "__main__":
    main()
