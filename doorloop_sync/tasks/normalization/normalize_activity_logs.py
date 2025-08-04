from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

# FIX: Pass __name__ to the logger to identify the module in logs.
logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw activity log data and upserts it into the
    doorloop_normalized_activity_logs table.
    """
    logger.info("Starting normalization for activity_logs...")
    supabase = get_supabase_client()

    # Fetch raw data
    raw_response = supabase.table("doorloop_raw_activity_logs").select("*").execute()
    raw_records = raw_response.data

    if not raw_records:
        logger.info("No raw activity_logs data to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        # Example normalization logic (adapt as needed)
        normalized_records.append({
            "doorloop_id": record.get("id"),
            "created_at": record.get("createdAt"),
            "user_id": record.get("userId"),
            "event": record.get("event"),
            "details": record.get("details"),
        })

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized activity log records...")
        supabase.table("doorloop_normalized_activity_logs").upsert(normalized_records).execute()
    else:
        logger.info("No records to upsert after normalization.")

