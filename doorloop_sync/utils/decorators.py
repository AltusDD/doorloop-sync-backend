# silent-tag: decorators-fix-0803
import functools
import logging

logger = logging.getLogger("ETL_Orchestrator")

def task_error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"--- TASK FAILED: {task_name} ---")
            logger.error(f"ERROR_TYPE: {type(e).__name__}")
            logger.error(f"ERROR_MSG: {e}")
            return None
    return wrapper
