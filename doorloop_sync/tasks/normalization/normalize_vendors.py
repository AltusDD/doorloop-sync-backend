from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

# FIX: Pass the module name to the logger.
logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw vendor data and upserts it into the
    doorloop_normalized_vendors table.
    """
    logger.info("Starting normalization for vendors...")
    # FIX: Use the factory function to get the client instance.
    supabase = get_supabase_client()

    raw_table_name = "doorloop_raw_vendors"
    normalized_table_name = "doorloop_normalized_vendors"

    raw_records = supabase.fetch_all(raw_table_name)

    if not raw_records:
        logger.info(f"No raw data in {raw_table_name} to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        # This normalization logic should be adapted to your actual schema
        normalized_data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
            "email": record.get("email"),
            "phone": record.get("phone"),
        }
        # Remove keys with None values to prevent schema conflicts
        normalized_records.append({k: v for k, v in normalized_data.items() if v is not None})

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table_name}...")
        supabase.upsert(table=normalized_table_name, data=normalized_records)
    else:
        logger.info("No records to upsert after normalization.")

