
def generate_default_args(owner, on_failure_callback):
    return {
        "owner": owner,
        "depends_on_past": False,
        "on_failure_callback": on_failure_callback,
        "spark_failure_msg": "Spark job failed"
    }