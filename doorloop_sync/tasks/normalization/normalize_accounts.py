from doorloop_sync.config import get_doorloop_client, get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

# FIX: Pass the module name to the logger.
logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Fetches all accounts from the DoorLoop API and upserts them into the
    doorloop_raw_accounts table in Supabase.
    """
    entity_name = "accounts"
    table_name = "doorloop_raw_accounts"
    
    logger.info(f"Starting raw sync for {entity_name}...")
    
    doorloop = get_doorloop_client()
    supabase = get_supabase_client()

    try:
        # FIX: Use the correct get_all() method.
        data = doorloop.get_all(entity_name)
        
        if data:
            logger.info(f"Fetched {len(data)} records for {entity_name}.")
            # The Supabase client now handles empty data checks.
            supabase.upsert(table_name, data)
        else:
            logger.info(f"No records found for {entity_name}.")
            
    except Exception as e:
        logger.error(f"❌ Error syncing {entity_name}: {e}")

