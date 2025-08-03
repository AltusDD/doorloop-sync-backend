import functools
import logging
import traceback

logger = logging.getLogger("ETL_Orchestrator")

def task_error_handler(task_name=None):
    def decorator(func):
        name = task_name or func.__name__
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"--- TASK FAILED: {name} ---")
                logger.error(f"ERROR_TYPE: {type(e).__name__}")
                logger.error(f"ERROR_MSG: {e}")
                # logger.error(f"TRACEBACK: {traceback.format_exc()}")
                return None
        return wrapper
    return decorator
