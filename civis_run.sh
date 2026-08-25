set -euo pipefail

# Deps are pinned on purpose. This job installed ccef-connections unpinned and without
# the [bigquery] extra; when 0.2.0 moved google-cloud-bigquery behind extras (2026-06-04)
# the job died at import every night for 82 nights. Bump the tag deliberately on upgrade.
pip install -r app/requirements.txt
pip install "ccef-connections[airtable,bigquery] @ git+https://github.com/common-cause/ccef_connections.git@v0.11.0"

python app/sync.py
