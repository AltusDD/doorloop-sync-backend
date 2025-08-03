
from doorloop_sync.config import get_supabase_client, get_logger
from utils.decorators import task_error_handler

@task_error_handler
def run():
    logger = get_logger()
    logger.info("Starting normalization for accounts...")
    supabase_client = get_supabase_client()
    # Add actual normalization logic here
    logger.info("Completed normalization for accounts.")
