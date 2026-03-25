# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

DAG definitions for the **UnIC** datalake at CHU Sainte-Justine. These DAGs orchestrate Spark ETL jobs defined in the companion `unic-etl` repository (located at `../unic-etl`). Airflow runs on Kubernetes; Spark jobs execute via `KubernetesPodOperator`.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Linting

```bash
pylint --load-plugins=pylint_airflow --fail-under=10.0 */**.py
```

CI runs this on every PR targeting `master`. The `.pylintrc` disables `line-too-long`, `pointless-statement` (needed for `op1 >> op2` Airflow syntax), `too-many-arguments`, and `too-many-branches`.

## Local Airflow (Docker)

```bash
cp .env.sample .env
docker-compose up
```

Access at `http://localhost:50080` (user: `airflow`, password: `airflow`). Upload `variables.json` via Admin → Variables to seed required variables. Test a task:

```bash
docker-compose exec airflow-scheduler airflow tasks test <dag_id> <task_id> 2022-01-01
```

## Architecture

### DAG creation — two patterns

**1. JSON-driven (preferred for most resources):** `dags/dags.py` walks `dags/config/<zone>/<subzone>/` and auto-generates a DAG for each `*_config.json` file. The DAG ID becomes `<subzone>_<resource_code>`. The full config schema is documented in `README.md`.

**2. Python DAGs:** Hand-written DAGs directly in `dags/` for advanced cases (e.g., `postgres_nrt_staturgence.py`, `unic_publish_project.py`, `iceberg_table_maintenance.py`).

### Data zones

Data flows through zones (storage in MinIO/S3):

- **red**: Nominative/PII data — subzones: `ingestion`, `curated`, `enriched`, `released`
- **yellow**: De-identified data (research project data and aggregated/pre-joined data) — subzones: `anonymized`, `enriched`, `warehouse`
- **green**: Staged and researcher-accessible data — subzones: `released` (staging to select a release candidate), `published` (researcher-accessible)

### Task execution

Each dataset in a config step becomes a `SparkOperator` task (`dags/lib/operators/spark.py`), which extends `KubernetesPodOperator`. It launches a Spark client pod in the `unic-<zone>` Kubernetes namespace using the `unic-etl` JAR from S3 (`s3a://spark-prd/jars/unic-etl-<branch>.jar`).

`SparkIcebergOperator` (`dags/lib/operators/spark_iceberg.py`) extends `SparkOperator` with an additional `iceberg-s3-credentials` volume mount, used by the Iceberg maintenance DAG.

`tasks.py::create_tasks()` wires tasks together: `dependencies` within a step express dataset-level ordering; steps themselves run sequentially.

### ETL versioning (V4 subzones)

Subzones `raw`, `curated`, `released`, `enriched`, and `anonymized` use ETL v4 argument format (`--config`, `--steps`, `--app-name`, `--destination`). Only `published` uses legacy positional args. Defined in `config.py::V4_SUBZONES`.

### Published projects

A step with `destination_subzone: "published"` (and no `main_class`) does not run Spark directly — it triggers the `unic_publish_project_<env>` DAG. That DAG copies Parquet to Excel, updates the data dictionary in Postgres, and indexes metadata into OpenSearch via three aliases (`resource_centric`, `table_centric`, `variable_centric`). OpenSearch index templates are defined in `dags/lib/templates/`.

One `unic_publish_project` DAG is generated per `PostgresEnv` (`qa`, `prod`). Similarly, `os_index_dags.py` generates one `os_<env>_index` DAG per `OpensearchEnv`.

### Key shared config (`dags/lib/config.py`)

Central constants for connection IDs, S3 bucket names, Spark class defaults, Jinja date/version templates, and Airflow `DEFAULT_ARGS`. The Airflow `dags_path` variable controls the root path; defaults to `/opt/airflow/dags/repo/dags`.

### Postgres DAGs

Several hand-written DAGs manage Postgres schema and data loads (e.g., `postgres_catalog_dags.py`, `postgres_nrt_staturgence.py`). They use `PostgresCaOperator` / `PostgresCaHook` (custom TLS-aware wrappers in `dags/lib/operators/` and `dags/lib/hooks/`). Two Postgres environments exist: `vlan2` (main UNIC DB) and `bi` (BI database).
