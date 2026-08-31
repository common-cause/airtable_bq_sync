---
name: assign-event-states
description: >
  Judgment pass over Event Reports in the 1 Million Conversations Airtable base:
  decide which US state each event happened in, from its free-text location and
  the host's home state, and write the two-letter code into Event State so the
  event can appear on the campaign map. Designed for scheduled headless runs;
  also invocable interactively. Use when asked to "assign event states", "map the
  new event reports", or as the recurring event-state maintenance pass.
---

# Assign event states

Fill `Event State` on Event Reports that don't have one. An event with no state
cannot be placed on the campaign map — its attendee count is simply missing from
the state totals, silently.

This is a judgment pass, not a parser. The location field is free text a
volunteer typed, and the interesting cases are the ones no pattern reaches: a
venue whose city you happen to know, a national march, a Zoom call with no
geography at all. Use what you know about the world, and say so in your
reasoning.

All Airtable I/O goes through `assign_event_states.py`, which makes no decisions
of its own. Run it from the project root.

## Hard rules

1. **Never overwrite an existing `Event State`.** Those are human adjudications.
   The tool refuses this at write time, but do not try — a row that already has
   a state is not your business.
2. **When you cannot decide, leave it blank and report it.** A blank row waits
   for a human and costs nothing. A wrong state silently misattributes an
   event's attendees to the wrong place on a public-facing map, and nobody will
   notice. The costs are asymmetric — bias hard toward leaving it blank.
3. **Two-letter USPS codes only** (`PA`, `NC`, `DC`). Never a full state name.
4. **Never invent a host.** If the host isn't in the Hosts table, you have no
   host signal; say so rather than guessing from their name or email.
5. **No PII in the run report.** Event Reports carry volunteer names and emails.
   Report record ids, locations, states, and counts. A host may be named where
   it is the reason for a decision ("host is OR staff") — never their email.

## The rubric

Work down this ladder. Stop at the first rung that gives a confident answer.

**1. An explicit state in the location text.**
`Camden, NC` → `NC`. `Milford, PA` → `PA`. This wins outright, including over
the host's own state — staff run events outside their home state routinely.

⚠️ **Read the whole cell before you conclude.** `Delaware Valley Action! office
in Milford, PA` is in **Pennsylvania**. The word "Delaware" is part of an
organization's name, and the Delaware Valley is a region spanning three states.
This exact row was assigned DE by a human skimming the first two words, and
corrected later. A state name inside a proper noun is not a location.

Related traps: the English words *in*, *or*, *me*, *de* are not Indiana, Oregon,
Maine or Delaware. "Washington" alone is ambiguous between the state and DC — see
rung 4.

**2. A virtual event.**
`Zoom`, `Virtual`, `Virtually over Zoom`, `Online`, `Phone call`, `Google Meet`.
The text describes a medium, not a place, so it carries no geography at all.
Use the **host's home state** — that is where the organizing effort belongs, and
it is what the existing rows do (three of the six).

**3. A real place with no state named.**
`First Unitarian Portland` — a venue, so not virtual, but no state given.
Two sources of signal, and you should use both:
  - What you know: First Unitarian is a well-known Portland congregation, and
    Portland OR is far larger than Portland ME.
  - The host's home state, which here is OR and agrees.
When your own knowledge and the host's state **agree**, assign it. When they
**disagree**, that is a genuine ambiguity — leave it blank and report both
readings. Do not silently pick one.

**4. A named national event.**
`March on Washington` is in **DC**, not Washington state. Treat well-known
national mobilizations by where they actually happen. If you are not confident
the event is the famous one, drop to rung 5.

**5. Nothing usable.**
Empty location, or text so vague it could be anywhere (`Personal conversation`,
`Online` with no host in the Hosts table). Leave blank, report why.

## Procedure

### 1. Find the candidates

```bash
python assign_event_states.py --list
```

Returns JSON: `record_id`, `location`, `event_name`, `attendee_count`,
`host_name`, `host_email`, `host_home_state`, `host_in_hosts_table`.

An empty list is **"all events mapped, nothing to assign"** — a healthy quiet
run. Report it as such and stop; it is not a failure.

### 2. Decide each one

Apply the rubric. For every row write down, before deciding:
  - the rung you landed on,
  - the state you chose,
  - what made you confident.

If you cannot complete that sentence honestly, the answer is blank.

If `host_in_hosts_table` is false and you needed the host signal, that is a
**Hosts-table gap**, not a failure of this pass. Report the host's name and the
state you'd have needed, so it can be added by
`python seed_host_states.py --apply` (which reads the knowledge library staff
directory) or by hand.

### 3. Write

One `--set` per record, batched into a single call:

```bash
python assign_event_states.py --set recAAA=PA --set recBBB=NC
```

The tool re-reads each row immediately before writing and skips anything that
gained a state in the meantime, so this is safe to re-run after a partial
failure. It exits 1 if anything was skipped — read the `skipped` list, don't
just retry.

Write **only** the rows you decided. Leave the rest alone.

### 4. Verify

```bash
python assign_event_states.py --list
```

The rows you assigned must be gone from the list. Anything still present that
you thought you wrote is a real problem — report it, do not attempt a fix.

The values reach BigQuery on the next sync (`million_conversations.event_reports`,
daily 3:00 AM ET). You do not need to run the sync, and should not.

### 5. Report

- Count assigned, by state.
- Every assignment: record id, the location text, the state, and the rung +
  reasoning that produced it.
- Every row left blank: record id, location, and precisely what was ambiguous.
- Any Hosts-table gaps, named.
- The verification re-list returning clean.
- "All events mapped" when there was nothing to do.

## Escalation — stop, diagnose, do not retry

- The `Hosts` table is missing entirely (`host_home_state` null on every row and
  `host_in_hosts_table` false throughout). Rungs 2 and 3 are unavailable; say so
  rather than falling back to guesses.
- `--set` reports a record as already assigned that you know was blank in step 1
  — someone is editing concurrently, and your read is stale.
- More than half the candidates land on rung 5. That suggests the form changed
  or a new kind of location text appeared, and the rubric needs a human look
  rather than a best effort.
