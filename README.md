# unic-dag

## how access airflow

#### Forward 8080 port to your local machine

```
kubectl port-forward svc/airflow-web 8888:8080 -n airflow
```

### access the web-ui on localhost

```
http://localhost:8888
```

## how to run a dag

To run production dag, simply hit the play button of a specific dag
To debug a dag, 
