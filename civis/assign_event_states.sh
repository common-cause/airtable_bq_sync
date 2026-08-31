set -euo pipefail

# Assigns Event State on Event Reports rows that lack one, then exits. Writes to
# Airtable only — no BigQuery — so this installs the [airtable] extra alone.
#
# Keep the ccef-connections tag in step with civis_run.sh. They are pinned
# separately because the two jobs need different extras, but they should move
# together: an unpinned, extras-less install is what killed the sync for 82
# consecutive nights (2026-06-04).
pip install -r app/requirements.txt
pip install "ccef-connections[airtable] @ git+https://github.com/common-cause/ccef_connections.git@v0.11.0"

# MUST run before the 3:00 AM sync, not after: the sync is what carries the new
# Event State values into BigQuery. Reversed, every assignment sits a full day
# behind in the warehouse.
#
# Add --fail-on-unresolved to turn "a row needs a human" into a failure email.
# Left off by default so a red job keeps meaning "broken", not "review me".
python app/assign_event_states.py --apply
