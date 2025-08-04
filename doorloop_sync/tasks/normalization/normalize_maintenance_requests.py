from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw maintenance_requests data from Supabase and upserts it into the
    doorloop_normalized_maintenance_requests table.
    """
    entity_name = "maintenance_requests"
    raw_table = "doorloop_raw_maintenance_requests"
    normalized_table = "doorloop_normalized_maintenance_requests"

    logger.info(f"Starting normalization for {entity_name}...")
    supabase = get_supabase_client()

    raw_records = supabase.fetch_all(raw_table)

    if not raw_records:
        logger.info(f"No raw data in {raw_table} to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        normalized_data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
        }
        normalized_records.append({k: v for k, v in normalized_data.items() if v is not None})

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table}...")
        supabase.upsert(table=normalized_table, data=normalized_records)
    else:
        logger.info("No records to upsert after normalization.")
