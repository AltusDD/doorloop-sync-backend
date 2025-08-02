import logging
# --- CORRECTED IMPORT PATTERN ---
# All tasks get their dependencies from the centralized config module.
from doorloop_sync.config import get_doorloop_client, get_supabase_client, get_logger

# Use a logger specific to this task for clear log filtering
logger = get_logger(__name__)

def run():
    """
    Fetches raw account data from DoorLoop and upserts it into the raw Supabase table.
    """
    logger.info("Starting raw sync for accounts...")
    
    try:
        # Get initialized clients from the config
        doorloop_client = get_doorloop_client()
        supabase_client = get_supabase_client()
        
        api_endpoint = "accounts"
        raw_table_name = "doorloop_raw_accounts"

        # 1. Fetch data from DoorLoop
        data = doorloop_client.get_all(api_endpoint)
        if not data:
            logger.info("No account data returned from API. Task complete.")
            return

        # 2. De-duplicate records based on 'id' to prevent upsert errors
        unique_records = list({item['id']: item for item in data}.values())
        
        # 3. Prepare records for insertion
        records_to_insert = [
            {"id": record.get("id"), "data": record, "source_endpoint": api_endpoint} 
            for record in unique_records
        ]
        
        # 4. Upsert data to Supabase
        supabase_client.upsert(table=raw_table_name, data=records_to_insert)
        logger.info(f"Successfully synced {len(records_to_insert)} raw account records.")

    except Exception as e:
        logger.error(f"An error occurred during raw account sync: {e}", exc_info=True)
        # Re-raise the exception to allow the orchestrator to catch it
        raise

# This block allows the script to be run directly for testing
if __name__ == "__main__":
    run()
