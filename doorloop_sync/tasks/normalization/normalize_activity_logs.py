import logging
# --- CORRECTED IMPORT PATTERN ---
# All tasks get their dependencies from the centralized config module.
from doorloop_sync.config import get_supabase_client, get_logger

logger = get_logger(__name__)

def run():
    """
    Normalizes raw activity log data from Supabase and upserts it into the
    normalized table.
    """
    entity = "activity_logs"
    logger.info(f"🔄 Starting normalization for {entity}")

    try:
        # Get an initialized Supabase client from the config
        supabase_client = get_supabase_client()
        
        raw_table_name = "doorloop_raw_activity_logs"
        normalized_table_name = "doorloop_normalized_activity_logs"

        # 1. Fetch raw data from Supabase
        response = supabase_client.supabase.table(raw_table_name).select("data").execute()
        raw_records = response.data
        
        if not raw_records:
            logger.info(f"📭 No raw data found for {entity}. Task complete.")
            return

        # 2. Normalize the data
        normalized_records = []
        for record in raw_records:
            raw = record.get("data", {}) # The JSON payload is in the 'data' column
            normalized = {
                "doorloop_id": raw.get("id"),
                "entity_type": raw.get("entityType"),
                "entity_id": raw.get("entityId"),
                "action": raw.get("action"),
                "user_id": raw.get("userId"),
                "timestamp": raw.get("timestamp"),
            }
            normalized_records.append(normalized)

        # 3. Upsert the normalized records
        if normalized_records:
            supabase_client.upsert(table=normalized_table_name, data=normalized_records)
            logger.info(f"✅ Successfully normalized and upserted {len(normalized_records)} records for {entity}.")
            # Note: The orchestrator will handle audit logging for success/failure of the task.

    except Exception as e:
        logger.error(f"❌ Failed to normalize {entity}: {e}", exc_info=True)
        # Re-raise the exception so the orchestrator can catch and log it.
        raise

# This block allows the script to be run directly for testing
if __name__ == "__main__":
    run()
