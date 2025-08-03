
from doorloop_sync.config import get_supabase_client, get_logger
from utils.decorators import task_error_handler

@task_error_handler
def run():
    logger = get_logger()
    logger.info("Starting normalization for properties...")
    supabase_client = get_supabase_client()
    logger.info("Completed normalization for properties.")

# silent_update
