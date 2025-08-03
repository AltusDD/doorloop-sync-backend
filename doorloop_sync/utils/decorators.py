
import logging
from functools import wraps

def task_error_handler(task_name="unnamed_task"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(task_name)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"❌ Task '{task_name}' failed due to: {e}", exc_info=True)
                raise e
        return wrapper
    return decorator
