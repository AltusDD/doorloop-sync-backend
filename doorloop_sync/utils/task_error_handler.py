
import functools
import logging
import traceback

logger = logging.getLogger("ETL_Orchestrator")

def task_error_handler(func):
    """
    A decorator that wraps an ETL task to catch, log, and handle exceptions
    in a standardized way.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"--- TASK FAILED: {task_name} ---")
            logger.error(f"ERROR_TYPE: {type(e).__name__}")
            logger.error(f"ERROR_MSG: {e}")
            # logger.error(f"TRACEBACK: {traceback.format_exc()}")
            return None
    return wrapper
