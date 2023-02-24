def generate_default_args(owner, on_failure_callback, on_success_callback=None, on_execute_callback=None):
    return {
        "owner": owner,
        "depends_on_past": False,
        "execution_timeout": 3,
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback,
        "on_execute_callback": on_execute_callback
    }
