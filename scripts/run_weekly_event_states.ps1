# Weekly event-state assignment pass - invoked by Windows Task Scheduler
# ("Airtable BQ Sync - weekly event-state agent", Mondays 02:40 local).
# One agent pass, dispatched THROUGH THE TOWER. Output lands in
# logs\event_states\<timestamp>.log (gitignored, newest 60 kept).
# NOTE: keep this file pure ASCII - PS 5.1 reads BOM-less files as ANSI, and a
# mojibaked em-dash or smart quote inside a string breaks the parser.
#
# WHY THIS SLOT (contract .claude\dispatch.yaml -> schedule). Monday 02:40 is
# Sunday overnight: the states are set before the week starts working off them,
# and 02:40 is ahead of the 3:00 AM Civis sync that carries Event State into
# BigQuery. Fire it after that sync and every assignment sits a full WEEK stale
# in the warehouse. Weekly rather than nightly because new event reports arrive
# at roughly one a week, and a loop that reports "nothing to do" six nights in
# seven teaches its own heartbeat to be ignored.
#
# THIS SCRIPT IS THE VEHICLE, NOT THE DISPATCHER. It owns the slot, the log and
# the retention; the tower owns the run. The tower does: freeze consult
# (fail-closed) -> grant gate -> fire-time request synthesis -> brief assembly
# with the contract verbatim -> pre-spawn validation -> spawn -> run-report
# parse -> ledger append -> tier-M checks -> Asana projection.
#
# Do NOT "simplify" this to:
#     '' | & "$env:USERPROFILE\.local\bin\claude.exe" -p "/assign-event-states"
# That form bypasses the tower entirely. It would still assign states, and the
# run would be invisible to the ledger, uncounted by the error budget, and
# outside the break-in review Rob is using to check the first three writes.
# Treaty 2.8. Do not restore it without saying so out loud.
#
# A tower-side failure means no pass this week, and that is the intended
# behaviour: an unassigned event is silently absent from state totals until the
# next pass, which is recoverable, while a run that dodges the law to stay green
# is not. There is deliberately no fallback path.

$ErrorActionPreference = 'Continue'

# Encoding, end to end. Under Task Scheduler python defaults to cp1252 for
# stdout and PS 5.1 decodes native output as the OEM code page, which has
# already crashed the tower once (2026-08-26) printing a diagnosis containing
# U+2192, after the ledger row landed but before the Asana projection. Make
# every python child emit UTF-8 and make PS decode it as UTF-8.
$env:PYTHONIOENCODING = 'utf-8'
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

$taskType = 'assign-event-states'

$proj   = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $proj 'logs\event_states'
New-Item -ItemType Directory -Force $logDir | Out-Null
$log    = Join-Path $logDir ((Get-Date -Format 'yyyy-MM-dd_HHmm') + '.log')

Set-Location $proj

# Tower location and interpreter. Local-only job by contract (the rubric needs
# world knowledge and the Hosts seeding needs the knowledge library, neither of
# which a container has), so a machine path with an env override is the right
# amount of configuration.
$tower = $env:TOWER_ROOT
if (-not $tower) {
    $tower = Join-Path $env:USERPROFILE 'OneDrive - Common Cause Education Fund\Documents\Local AI Tools\RobAssistant'
}
$towerPy = $env:ASSISTANT_PYTHON
if (-not $towerPy) { $towerPy = 'C:\venvs\rob-assistant\Scripts\python.exe' }
$fire = Join-Path $tower 'scripts\dispatch_fire.py'

if (-not (Test-Path $fire)) {
    "=== TOWER MISSING at $fire - nothing dispatched this week ===" |
        Out-File -Append -Encoding utf8 $log
    exit 1
}

"=== dispatch airtable-bq-sync :: $taskType - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" |
    Out-File -Append -Encoding utf8 $log

# ToString flattens PS 5.1's ErrorRecord-wrapped stderr lines. The tower owns
# the spawn, so no stdin plumbing is needed here.
& $towerPy $fire 'airtable-bq-sync' $taskType 2>&1 |
    ForEach-Object { $_.ToString() } |
    Out-File -Append -Encoding utf8 $log
$code = $LASTEXITCODE

# 0 = completed or a diagnosed rejection (both successes under law),
# 1 = failed, 2 = never spawned because assembly failed (work item parked).
"--- exit=$code at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ---" |
    Out-File -Append -Encoding utf8 $log

# Retention: keep the newest 60 run logs (at a weekly cadence, over a year).
Get-ChildItem $logDir -Filter *.log |
    Sort-Object Name -Descending |
    Select-Object -Skip 60 |
    Remove-Item -Force

exit $code
