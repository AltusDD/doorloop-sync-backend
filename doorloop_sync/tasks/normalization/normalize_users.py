from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw user data and upserts it into the
    doorloop_normalized_users table.
    """
    logger.info("Starting normalization for users...")
    supabase = get_supabase_client()
    
    raw_table_name = "doorloop_raw_users"
    normalized_table_name = "doorloop_normalized_users"

    raw_records = supabase.fetch_all(raw_table_name)

    if not raw_records:
        logger.info(f"No raw data in {raw_table_name} to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        # FIX: Remove the 'balance' field as it does not exist in the target table.
        normalized_data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
            "type": record.get("type"),
            # 'balance': record.get("balance"), # This column does not exist
        }
        # Remove keys with None values to prevent other potential schema issues
        normalized_records.append({k: v for k, v in normalized_data.items() if v is not None})

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table_name}...")
        supabase.upsert(table=normalized_table_name, data=normalized_records)
    else:
        logger.info("No records to upsert after normalization.")

