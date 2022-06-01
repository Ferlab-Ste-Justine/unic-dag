# unic-dag

## how to run a DAG

### port forward

```
kubectl port-forward svc/airflow-web 8888:8080 -n airflow
```

### go to the web-ui

```
https://localhost:8888
```

### Run DAG with config

In the JSON config, you can specify the following parameters to overwrite these default values:
```json
{
  "branch": "master",
  "imageVersion": "3.0.0_1"
}
```