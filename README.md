# UNIC dag

## Set up Python virtual environment

Create venv :
```bash
python -m venv venv
```
Activate venv :
```bash
source venv/bin/activate
```
Install requirements :
```bash
pip install -r requirements
```

## Run DAG with config

Choose a DAG and select _Trigger DAG w/ config_. In the JSON config, you can specify the following parameters to overwrite these default values:
```json
{
  "branch": "master",
  "version": "latest"
}
```
## Release a version
To release a version of an enriched dataset and publish it to researchers, run the enriched DAG with a config specifying the version number.
```json
{
  "version": "1.0.0"
}
```
The version number must follow this format : `"x.x.x"` where _x_ is a number.

## Run Airflow locally with Docker

Create `.env` file :

```bash
cp .env.sample .env
```

Deploy stack :

```bash
docker-compose up
```

Login to Airflow UI :

- URL : `http://localhost:50080`
- Username : `airflow`
- Password : `airflow`

Create Airflow variables (__Airflow UI__ => __Admin__ => __Variables__) :

- dags_path : `/opt/airflow/dags`
- base_url (optional) : `http://localhost:50080`

_For faster variable creation, upload the `variables.json` file in the Variables page._

### Test one task

```bash
docker-compose exec airflow-scheduler airflow tasks test <dag> <task> 2022-01-01
```

### _Optional_ : Set up Minio

Login to MinIO console :

- URL : `http://localhost:59001`
- Username : `minioadmin`
- Password : `minioadmin`

Create Airflow variable (__Airflow UI__ => __Admin__ => __Variables__) :

- s3_conn_id : `minio`

Create Airflow connection (__Airflow UI__ => __Admin__ => __Connections__) :

- Connection Id : `minio`
- Connection Type : `Amazon S3`
- Extra :

```json
{
  "host": "http://minio:9000",
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin"
}
```

### _Optional_ : Set up Slack

Create Airflow variable (__Airflow UI__ => __Admin__ => __Variables__) :

- slack_hook_url : `https://hooks.slack.com/services/...`