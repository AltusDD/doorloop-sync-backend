from functools import wraps
import logging

def task_error_handler(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        name = fn.__name__
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logging.error(f"❌ Error syncing {name}: {e}")
    return wrapper
