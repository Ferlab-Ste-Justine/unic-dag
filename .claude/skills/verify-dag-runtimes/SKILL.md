---
name: verify-dag-runtimes
description: Query prod Airflow for the last N successful runs of a set of DAGs, compute avg / min / max durations, and produce a markdown report. Use this before proposing a schedule change, before updating runtime estimates in Notion, or any time someone asks "how long does X actually take in prod?".
---

# Verify DAG runtimes from prod Airflow

## What this produces

A markdown table comparing actual prod runtimes (avg of last N successful runs) to an optional expected runtime, with min/max and a variance flag. Use it to:

- Refresh runtime estimates for the Notion ingestion schedule
- Back a proposed schedule reshuffle with real data
- Detect DAGs that have drifted (actual much slower than documented) or have no recent successful runs

## Prerequisites

- `kubectl` context is set to the prod cluster. Run `kubectl config current-context` — the exact context name varies per user, but it will contain the tokens `unic`, `prod`, and `etl` (e.g. `unic.prod.etl`).
- You have exec access to namespace `unic-prod`

If the context isn't set, ask the user to switch contexts themselves — don't switch it for them.

## Steps

### 1. Gather inputs from the user

Ask — don't assume — for:

- **DAG list.** Either: specific DAG IDs, "all DAGs on the Notion schedule", or "all active non-paused DAGs." If the user points to the Notion page, fetch it and extract the red (our DAGs) and green (project DAGs) entries.
- **N (runs to average over).** Default: 5. More runs = more stable average but stale runs may no longer reflect current data volumes.
- **Expected runtimes (optional).** If the user wants a comparison, ask where (Notion page URL, or explicit list).

### 2. Discover the adhoc pod dynamically

The pod name has a rolling hash. Always look it up:

```bash
POD=$(kubectl -n unic-prod get pods -l app=airflow-adhoc-queries -o jsonpath='{.items[0].metadata.name}')
```

### 3. Pull successful run history per DAG

```bash
kubectl -n unic-prod exec "$POD" -- airflow dags list-runs -d <DAG_ID> --state success -o json
```

The output is a JSON array of runs, newest first, each with `start_date` and `end_date` in ISO 8601. Parse the first N entries.

Serial `kubectl exec` calls are fine — ~1–2s each. For ~30 DAGs this takes about a minute. Don't try to parallelize unless the user asks; large parallel exec fan-outs can trip rate limits.

### 4. Compute stats

For each DAG's N runs: `duration = end_date - start_date` (minutes). Compute `avg`, `min`, `max`.

Apply these verdict rules:

| Condition | Verdict |
|---|---|
| 0 successful runs | "no data" — report separately |
| `avg > 1.5 * expected` or `avg - expected > 15 min` | ⚠️ over |
| `avg < 0.5 * expected` and `expected - avg > 5 min` | ✨ under — estimate may be too generous |
| `max - min > 30 min` | high variance (note alongside the verdict) |
| otherwise | ✓ close |

Print durations in human-friendly format: `6m`, `1h07`, `3h15`. Under 1 min → seconds.

### 5. Output a markdown report

Structure:

```
# Average runtime report (last N successful runs per DAG)

## Within tolerance ✓
| DAG | Expected | Avg | Min–Max |
|---|---|---|---|
...

## Notion overstates ✨
...

## Notion understates ⚠️
...

## No usable data
- `curated_cscmed_jobs` — 0 runs in history (explain why if known)
```

If the user didn't provide expected values, drop the "Expected" column and just show avg/min/max.

### 6. Flag special cases

- **Highly variable DAGs** (max > 2× avg, e.g. `enriched_sprintkid`): call out explicitly, note the worst case, and suggest root-cause investigation.
- **DAGs with only failed runs recently**: check `--state failed` to see if the DAG is broken or the schedule just hasn't fired recently. Report failed-run durations as a sanity check on the expected runtime.
- **Brand-new DAGs with zero history**: just say so; don't try to infer.

## Known Notion-specific gotchas when comparing

- Notion entries of the form `pericalm4 (2 min)` under a blue cell are IT loads, not our DAGs — don't match them to our Airflow DAG IDs.
- `(1/4)` on a Notion entry means monthly cadence. The DAG usually has 4× fewer runs to average over; N=5 might pull runs from a year back. Mention the staleness risk.
- Some Notion names don't map 1:1 to dag_ids: `index_patient` = `curated_unic`, `warehouse` = `warehouse_unic`, `clinibase-ci` = `anonymized_clinibaseci`. Ask the user if a mapping isn't obvious.
