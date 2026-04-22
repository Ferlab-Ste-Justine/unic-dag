---
name: verify-schedule-deployed
description: After a schedule-change PR is merged and deployed, verify that prod Airflow has re-parsed each DAG and that `schedule_interval` + `next_dagrun_create_after` match the new target. Flags the known gotcha where Python DAGs using `schedule_interval=timedelta(...)` keep the old anchor and need DAG-history clearing. Use right after merging a schedule-update PR.
---

# Verify schedule changes are live in prod Airflow

## What this produces

A per-DAG verification table confirming each changed DAG is running on its new schedule in prod, with explicit warnings for DAGs where:

- Airflow hasn't yet reconciled the new cron (scheduler lag — usually benign)
- A Python `timedelta`-scheduled DAG is still using its old anchor time (requires manual history clear)

## When to use this

- Right after a schedule-change PR is merged, once you see the merge commit on master and git-sync has had ~1–2 minutes to pull it
- Any time you suspect a DAG is still running on an old schedule despite the repo showing a newer one

## Prerequisites

- `kubectl` context is set to the prod cluster. Run `kubectl config current-context` — the exact context name varies per user, but it will contain the tokens `unic`, `prod`, and `etl`.
- You have exec access to namespace `unic-prod`
- You know which DAGs changed and what their new schedule should be (extract from the PR diff if the user doesn't supply a list)

## Steps

### 1. Gather the change list

From the user, the PR, or by looking at `git log --stat` on the merge commit: the set of `(dag_id, expected schedule)` pairs. For cron DAGs the expected schedule is a cron string. For Python timedelta DAGs the expected schedule is the target hour (in Montreal local time).

### 2. Discover the adhoc pod

```bash
POD=$(kubectl -n unic-prod get pods -l app=airflow-adhoc-queries -o jsonpath='{.items[0].metadata.name}')
```

### 3. Query each DAG's current state from Airflow

```bash
kubectl -n unic-prod exec "$POD" -- airflow dags details <DAG_ID> -o json
```

Extract these fields (they're all inside the first element of the returned JSON array):

- `schedule_interval` — dict with `__type: "CronExpression"` or `__type: "TimeDelta"` and a value
- `timetable_description` — human-readable (e.g. "At 00:00, only on Tuesday")
- `last_parsed_time` — when Airflow last re-read the DAG file (should be after the merge)
- `next_dagrun_data_interval_end` — when the next run will actually fire (UTC)
- `next_dagrun_create_after` — when the scheduler will create the next dagrun row (UTC; sometimes `null` for reasons explained below)

### 4. Compare against expected

**For cron DAGs:**

- `schedule_interval.value` must match the expected cron string exactly. If it doesn't, the deploy hasn't landed — check git-sync.
- `next_dagrun_data_interval_end` should fall on the expected day-of-week and time (convert UTC to Montreal). If yes, the cron is live.
- If `next_dagrun_create_after` is `null`, don't default to "scheduler lag." In practice there are **two** reasons this field can be null right after a schedule change:
  1. **Scheduler lag** — Airflow hasn't finished reconciling. Resolves within minutes.
  2. **The DAG is actively running a "late" triggered dagrun.** A schedule change that moves the fire time *backwards* (e.g. Tue 04:00 → Tue 00:00) can cause Airflow to treat a past interval as now-due and immediately kick off a run. `next_dagrun_create_after` stays null until that triggered run finishes.

  When you see `null`, check the Airflow UI or query `airflow dags list-runs -d <dag> --state running -o json`. If a run is in progress and its `data_interval_end` matches the new schedule, case (2) applies and the schedule is actually working correctly. If nothing is running, case (1) is more likely.

**For Python `timedelta` DAGs:**

- `schedule_interval.__type` will be `TimeDelta` (not CronExpression).
- The only way to change the time-of-day of these DAGs is via `start_date`. **Airflow may keep the old anchor** — observed behavior on this project: after a start_date-only change, the scheduler has kept the old interval anchor until DAG history is cleared manually. Treat this as expected until proven otherwise.
- Convert `next_dagrun_create_after` (UTC) to Montreal local and compare the hour to the expected hour.
- **If the hour matches** → ✓ the new `start_date` propagated.
- **If the hour is still the old value** → ⚠️ explicitly flag for the user. One known remediation is to clear DAG history in the Airflow UI so the scheduler re-anchors on the new `start_date`. **Do NOT recommend this lightly — include the warning below verbatim when you flag a timedelta DAG.**

  > ⚠️ **Clearing DAG history is irreversible and has side effects. Please confirm before proceeding:**
  > - **Past run records and task logs are deleted** and cannot be recovered.
  > - If the DAG has `catchup=True`, Airflow will **backfill every missed interval** from `start_date` forward on the next scheduler tick — potentially many runs, potentially hitting the same Spark cluster / external systems repeatedly.
  > - Any downstream metadata or reporting that keys on DAG run IDs or execution dates will lose its references.
  > - Other alternatives exist (custom `Timetable`, pause/unpause cycle, targeted SQL on the metadata DB). Discuss before defaulting to a full history clear.
  >
  > Do not perform the clear yourself — surface it as a recommendation for a human decision.

Timezone reminder: Montreal is UTC-4 in EDT (summer) and UTC-5 in EST (winter). If the DAG has been running across DST transitions on a `timedelta(weeks=4)` schedule, the UTC anchor may have drifted from the original local hour — don't be surprised if the "before" time doesn't cleanly equal the documented start_date hour.

### 5. Produce the verification table

```
# Verification report

All N DAGs were re-parsed at <last_parsed_time>.

## Cron DAGs
| DAG | Schedule in Airflow | Next run (Montreal) | Status |
|---|---|---|---|
| <dag_id> | <cron> | <day hh:mm> | ✓ / ⏳ / 🏃 / ⚠️ |

## Python timedelta DAGs
| DAG | New anchor hour | Airflow's next run | Status |
|---|---|---|---|
```

Legend:
- ✓ schedule matches and `next_dagrun_create_after` is populated with the correct time
- ⏳ cron correct but `next_dagrun_create_after` still null and no run in progress — scheduler lag, usually benign
- 🏃 cron correct and `next_dagrun_create_after` is null because a "late" triggered run is currently executing (schedule is working)
- ⚠️ mismatch — requires user action (history clear, redeploy, etc.)

### 6. Call out actions

End the report with a short "Action needed" section that lists only the ⚠️ items, each with the specific next step. For any timedelta-DAG anchor mismatch, include the irreversibility warning block verbatim (from step 4 above) before recommending a history clear — colleagues running this skill should see the full risk callout, not just a one-line "clear history".
