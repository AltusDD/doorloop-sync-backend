from functools import wraps
import logging

def task_error_handler(arg=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            name = arg if isinstance(arg, str) else fn.__name__
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logging.error(f"❌ Error syncing {name}: {e}", exc_info=True)
        return wrapper

    if callable(arg):
        return decorator(arg)  # Used as @task_error_handler
    return decorator           # Used as @task_error_handler("name")
