# doorloop_sync/utils/decorators.py

from functools import wraps
import logging

def task_error_handler(task_name=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            name = task_name or fn.__name__
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logging.error(f"❌ Error syncing {name}: {e}", exc_info=True)
        return wrapper
    return decorator
