# 🌬️ unic-dag

This repository contains the DAG definitions for the **UnIC** datalake project (_Univers Informationnel du CHU
Sainte-Justine_). These DAGs orchestrate ETL jobs defined in
the [unic-etl repository](https://github.com/Ferlab-Ste-Justine/unic-etl).

---

## ⚙️ Requirements

- 🐍 Python 3.9

### Setup with a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 🛠️ DAG configuration

DAGs can be defined in **two ways**:

1. JSON configuration files located in `dags/config`.
2. Python DAGs directly in the `dags` folder for advanced use cases.

### 📄 JSON configuration files

Each JSON file in `dags/config` defines one DAG to orchestrate a single **resource**. The file must follow this naming
and placement convention:

- The **filename** must start with the desired **dag_id** and end with `_config.json`. The convention is to use the *
  *resource_code** as the **dag_id**.
- The file must be placed under the folder for its **starting zone**:
    - `red` or `yellow`
- And inside the corresponding **starting subzone** folder:
    - `red` → `ingestion`, `curated`, `enriched`
    - `yellow` → `anonymized`, `enriched`, `warehouse`

The generated DAG ID will follow this pattern:

```
<subzone>_<resource_code>
```

> Example:  
> A file named `dags/config/red/curated/opera_config.json` will create a DAG named `curated_opera`.

#### JSON Configuration Fields

The root fields of the file are :

| Field           | Type   | Required | Default        | Description                                    |
|-----------------|--------|----------|----------------|------------------------------------------------|
| `concurrency`   | int    | ❌        | `1`            | Max number of parallel tasks.                  |
| `start_date`    | string | ❌        | `"2021-01-01"` | Initial DAG run date (format: `"YYYY-MM-DD"`). |
| `schedule`      | string | ❌        | –              | CRON expression for scheduling.                |
| `catchup`       | bool   | ❌        | `false`        | Whether to backfill missed DAG runs.           |
| `timeout_hours` | int    | ❌        | `4`            | Max execution time in hours before timeout.    |
| `steps`         | array  | ✅        | –              | List of execution steps. See below.            |

<details> <summary>Each <b>step</b> can contain</summary>

| Step Field              | Type   | Required | Default | Description                                                                               |
|-------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------|
| `destination_zone`      | string | ✅        | –       | Target zone (e.g., `"red"`, `"yellow"`, `"green"`).                                       |
| `destination_subzone`   | string | ✅        | –       | Subzone within the zone (e.g., `"curated"`, `"released"`).                                |
| `main_class`            | string | ❌        | –       | Main class to run in `unic-etl`. Optional for published tasks.                            |
| `multiple_main_methods` | bool   | ❌        | `false` | Whether multiple entrypoints exist in the main class.                                     |
| `pre_tests`             | array  | ❌        | `[]`    | QA tests before step execution. See below.                                                |
| `datasets`              | array  | ✅        | –       | List of datasets to process in the step. One task per dataset will be created. See below. |
| `post_tests`            | array  | ❌        | `[]`    | QA tests after step execution. See below.                                                 |
| `optimize`              | array  | ❌        | `[]`    | List of dataset IDs to optimize (i.e. Delta compaction).                                  |

</details>

<details> <summary>Each <b>dataset</b> can contain</summary>

| Dataset Field  | Type   | Required | Default | Description                                                                                                                                    |
|----------------|--------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `dataset_id`   | string | ✅        | –       | Dataset ID. Supports wildcards.                                                                                                                |
| `cluster_type` | string | ❌        | –       | Cluster size (`"xsmall"`, `"small"`, `"medium"` or `"large"`). Optional for published tasks.                                                   |
| `run_type`     | string | ❌        | –       | Execution type (`"default"` or `"initial"` to reset data). Optional for published tasks.                                                       |
| `pass_date`    | bool   | ❌        | –       | Whether to pass the execution date as a `--date` parameter (for enriched tasks) or a `--version` parameter (for released and published tasks). |
| `dependencies` | array  | ✅        | -       | List of dataset IDs to run upstream. Set to `[]` if there are no upstream dependencies.                                                        |

</details>


<details> <summary>Each <b>test</b> (pre or post) must contain</summary>

| Test Field     | Type   | Required | Default | Description                                      |
|----------------|--------|----------|---------|--------------------------------------------------|
| `name`         | string | ✅        | –       | Name of the QA test in `unic-etl`.               |
| `destinations` | string | ✅        | –       | List of dataset IDs to test. Supports wildcards. |

</details>

<details> <summary>Each <b>published</b> step can contain</summary>

| Step Field            | Type   | Required | Default                                | Description                                                                                                                                                         |
|-----------------------|--------|----------|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `destination_subzone` | string | ✅        | –                                      | Use `"published"` here to create a **published** step. This will create a task that triggers the `unic_publish_project` DAG with the necessary config.              |
| `resource_code`       | string | ❌        | The **filename** before `_config.json` | To overwrite the **filename** if it doesn't correspond to the **resource_code**.                                                                                    |
| `pass_date`           | bool   | ❌        | `false`                                | Whether to pass the execution date as the `--version` parameter to the `unic_publish_project` DAG. If `false`, `"latest"` will be passed. |
| `include_dictionary`  | bool   | ❌        | `false`                                | Whether to include the dictionary as an Excel file in the **published** bucket.                                                                                     |

</details>

### ✨Enriched DAGs Specifics

Project-DAGs (i.e., starting in the **enriched** subzone) can be divided into two types:

1. 1️⃣ **One-time projects** :

   One-time projects should define the **enriched** and **released** steps in their config file.
   When you are satisfied with a release candidate, you can manually trigger the `unic_publish_project` DAG.
2. 🔁 **Recurring projects** :

   Recurring projects should define the **enriched**, **released**, and **published** steps in their config file, as to
   automatically trigger the `unic_publish_project` DAG.

---

## ▶️ Starting a DAG from the Airflow UI

When manually triggering a DAG in the Airflow UI, you'll see two default input parameters:

| Parameter | Default  | Description                                                                                                    |
|-----------|----------|----------------------------------------------------------------------------------------------------------------|
| `branch`  | `master` | Selects the JAR file to run, corresponding to the branch used to deploy the JAR (e.g., `unic-etl-master.jar`). |
| `version` | `latest` | For **enriched** DAGs: set to a date (`YYYY-MM-dd`) to publish a specific version.                             |

---

## 🐳 Run Airflow Locally with Docker

#### 1. Create `.env` file

```bash
cp .env.sample .env
```

#### 2. Deploy the stack

```bash
docker-compose up
```

#### 3. Access the Airflow UI

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

#### 4. Set Airflow Variables

Navigate to **Admin → Variables** and add:

- dags_path : `/opt/airflow/dags`
- base_url (optional) : `http://localhost:50080`

To speed up setup, upload the `variables.json` file directly from the UI.

#### 5. Test a task

Run a task manually from the Airflow UI or use the CLI:

```bash
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

---

### 📦 Minio Setup _(Optional)_

#### 1. Access the Minio UI

- URL : `http://localhost:59001`
- Username : `minioadmin`
- Password : `minioadmin`

#### 2. Set Airflow Variable

Navigate to **Admin → Variables** and add:

- s3_conn_id : `minio`

#### 3. Set Airflow Connection

Navigate to **Admin → Connections** and add:

- Connection ID : `minio`
- Connection Type : `Amazon Web Services`
- Extra :

```json
{
  "endpoint_url": "http://minio:9000",
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin"
}
```

---

### 📦 Postgres Setup _(Optional)_

#### 1. Access the PgAdmin UI

- URL : `http://localhost:5050`
- Username : `pgadmin@pgadmin.com`
- Password : `pgadmin`

#### 2. Set Airflow Variable

Navigate to **Admin → Variables** and add:

- pg_conn_id : `postgres` (Set to corresponding Postgres connection ID. Eg: `unic_qa_postgresql_vlan2_rw`)

#### 3. Set Airflow Connection

Navigate to **Admin → Connections** and add:

- Connection ID : `postgres` (Set to corresponding Postgres connection ID. Eg: `unic_qa_postgresql_vlan2_rw`)
- Connection Type : `Postgres`
- Host: `postgres-unic`
- Schema: `unic`
- Password: `pgadmin`
- Port: `5432`

---

### 🔔 Slack Setup _(Optional)_

#### 1. Set Slack Hook Variable

Navigate to **Admin → Variables** and add:

- slack_hook_url : `https://hooks.slack.com/services/...`