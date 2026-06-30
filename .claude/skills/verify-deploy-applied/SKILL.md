---
name: verify-deploy-applied
description: After merging a PR to master, verify the change is live in prod Airflow — git-sync pulled the merge commit (scheduler + webserver), DAGs re-parsed afterwards, and there are 0 import errors. Use right after a merge to confirm deployment landed and refactors didn't break imports. For schedule-specific verification use verify-schedule-deployed instead.
---

# Verify a merged PR is applied in prod Airflow

## What this produces

A short all-clear report confirming that, in prod:

1. git-sync has pulled the merge commit (on both scheduler and webserver)
2. DAGs have been re-parsed *after* the sync
3. there are 0 DAG import errors

Together these prove the change is live and that the code imports/parses cleanly. This is **import/parse-time** confirmation only — runtime behavior inside operators' `execute()` is only proven when a task actually runs.

## When to use this

- Right after merging any PR to master (especially refactors/lint sweeps that touch `dags/lib/` modules imported by many DAGs), once you expect git-sync to have run (~1–2 min).
- Any time you suspect prod is running stale code.

## Prerequisites

- `kubectl` context is set to the prod cluster. Run `kubectl config current-context` — the name varies per user but contains the tokens `unic`, `prod`, and `etl`. Do **not** switch contexts on the user's behalf; just verify.
- Exec access to namespace `unic-prod`.

> **Critical: run these checks on the SCHEDULER pod, not the adhoc-queries pod.**
> `dags list` / `list-import-errors` re-parse the DAG folder *locally*, and the adhoc pod is missing packages (e.g. `openpyxl`), so it silently drops DAGs and reports phantom import errors. git-sync state also lives on the real pods. Use the scheduler pod (container `-c scheduler`). The adhoc pod is fine only for DB-backed queries (`dags details`, `list-runs`).

## Steps

### 1. Get the merge commit SHA on origin

```bash
git fetch origin master --quiet && git log origin/master -1 --oneline
```
Note the short SHA (e.g. `9eae26f`). This is the target every prod pod should be at.

### 2. Confirm git-sync pulled it — scheduler and webserver

The git-sync checkout dir is named by the synced commit SHA, and `repo` is a symlink to it.

```bash
SCHED=$(kubectl -n unic-prod get pods -l component=scheduler -o jsonpath='{.items[0].metadata.name}')
kubectl -n unic-prod exec -c scheduler "$SCHED" -- readlink /opt/airflow/dags/repo
# also note the sync time:
kubectl -n unic-prod exec -c scheduler "$SCHED" -- ls -la /opt/airflow/dags

WEB=$(kubectl -n unic-prod get pods -l component=webserver -o jsonpath='{.items[0].metadata.name}')
kubectl -n unic-prod exec -c webserver "$WEB" -- readlink /opt/airflow/dags/repo
```
Both symlink targets must start with the SHA from step 1. The webserver has its **own** git-sync sidecar (it is not synced by the same mechanism as scheduler/workers) — check it explicitly, since a stale webserver 500s on DAGs using custom timetables. If a target is still the old SHA, git-sync hasn't run yet — wait ~1 min and re-check.

### 3. Confirm DAGs re-parsed after the sync

Pick the DAGs the PR touched (from the diff). `dags details -o json` returns a **list** — read the first element.

```bash
for dag in <dag_id_1> <dag_id_2>; do
  echo -n "$dag  last_parsed: "
  kubectl -n unic-prod exec -c scheduler "$SCHED" -- airflow dags details "$dag" -o json \
    | python3 -c "import sys,json; d=json.load(sys.stdin); d=d[0] if isinstance(d,list) else d; print(d['last_parsed_time'])"
done
```
Each `last_parsed_time` should be **later than the repo symlink's mtime** from step 2. If a DAG parsed before the sync, the processor hasn't re-read it yet — wait and re-check (the DAG processor runs on a timer).

### 4. Confirm 0 import errors (the real all-clear)

```bash
kubectl -n unic-prod exec -c scheduler "$SCHED" -- airflow dags list-import-errors
```
Want: `No data found`. Any error here means a changed module fails to import — the refactor broke something. Cross-check the total DAG count is what you expect:
```bash
kubectl -n unic-prod exec -c scheduler "$SCHED" -- airflow dags list -o json | python3 -c "import sys,json; print(len(json.load(sys.stdin)))"
```

## Report

Summarize: synced SHA + sync time (scheduler & webserver both matching origin), re-parse times for the touched DAGs (all after sync), import-error count (0), total DAG count. State plainly that this confirms parse-time correctness only, and that runtime behavior is verified when the next task run completes.
