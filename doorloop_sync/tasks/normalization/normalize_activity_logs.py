from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

# FIX: Pass the module name to the logger.
logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw activity log data and upserts it into the
    doorloop_normalized_activity_logs table.
    """
    logger.info("Starting normalization for activity_logs...")
    supabase = get_supabase_client()
    
    raw_table_name = "doorloop_raw_activity_logs"
    normalized_table_name = "doorloop_normalized_activity_logs"

    # FIX: Use the fetch_all method from our SupabaseClient wrapper.
    raw_records = supabase.fetch_all(raw_table_name)

    if not raw_records:
        logger.info(f"No raw data in {raw_table_name} to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        normalized_records.append({
            "doorloop_id": record.get("id"),
            "created_at": record.get("createdAt"),
            "user_id": record.get("userId"),
            "event": record.get("event"),
            "details": record.get("details"),
        })

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table_name}...")
        # FIX: Use the upsert method from our SupabaseClient wrapper.
        supabase.upsert(table=normalized_table_name, data=normalized_records)
    else:
        logger.info("No records to upsert after normalization.")
