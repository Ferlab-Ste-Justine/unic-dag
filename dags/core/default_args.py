def generate_default_args(owner, on_failure_callback, on_success_callback=None):
    return {
        "owner": owner,
        "depends_on_past": False,
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback
    }
