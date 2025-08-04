from doorloop_sync.config import get_doorloop_client, get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

logger = get_logger(__name__)

@task_error_handler
def run():
    entity_name = "communications"
    table_name = f"doorloop_raw_{entity_name}"

    logger.info(f"Starting raw sync for {entity_name}...")

    doorloop = get_doorloop_client()
    supabase = get_supabase_client()

    try:
        data = doorloop.get_all(entity_name)

        if data:
            logger.info(f"Fetched {len(data)} records for {entity_name}.")
            supabase.upsert(table_name, data)
        else:
            logger.info(f"No records found for {entity_name}.")
    except Exception as e:
        logger.error(f"❌ An error occurred during the sync for {entity_name}: {e}")
