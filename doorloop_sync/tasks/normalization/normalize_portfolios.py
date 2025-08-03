from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler
from doorloop_sync.utils.transform import flatten_dict

logger = get_logger(__name__)

@task_error_handler(task_name="normalize_portfolios")
def run():
    logger.info("Starting normalization for portfolios...")

    supabase_client = get_supabase_client()

    raw_table_name = "doorloop_raw_portfolios"
    normalized_table_name = "doorloop_normalized_portfolios"

    raw_records = supabase_client.fetch_all(raw_table_name)
    if not raw_records:
        logger.info("No raw portfolios data to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        flat = flatten_dict(record)
        normalized = {
            "doorloop_id": flat.get("id"),
            "name": flat.get("name"),
            "notes": flat.get("notes"),
            "created_at": flat.get("createdAt"),
            "updated_at": flat.get("updatedAt"),
        }
        normalized_records.append(normalized)

    supabase_client.upsert(table=normalized_table_name, data=normalized_records)
    logger.info(f"✅ Normalized {len(normalized_records)} portfolios.")
