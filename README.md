# unic-dag

## How to run a DAG


1. Port forward the web UI
```
kubectl port-forward svc/airflow-web 8888:8080 -n airflow
```
2. Go to the web UI

```
https://localhost:8888
```
3. Choose a DAG and select _Trigger DAG_.

### _Optional_ : Run DAG with config

Choose a DAG and select _Trigger DAG w/ config_. In the JSON config, you can specify the following parameters to overwrite these default values:
```json
{
  "branch": "master",
  "imageVersion": "3.0.0_1",
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
