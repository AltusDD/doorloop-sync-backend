from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler
from doorloop_sync.utils.transform import flatten_dict

logger = get_logger()

@task_error_handler("normalize_recurring_credits")  # silent tag added to trigger GitHub update
def run(raw_records: list[dict]) -> list[dict]:
    if not raw_records:
        logger.info("No raw recurring_credits data to normalize. Task complete.")
        return []

    logger.info(f"Normalizing {len(raw_records)} recurring_credits records...")

    normalized_records = []
    for record in raw_records:
        flat = flatten_dict(record)
        flat["doorloop_id"] = record.get("id")
        normalized_records.append(flat)

    supabase_client = get_supabase_client()
    normalized_table_name = "doorloop_normalized_recurring_credits"
    supabase_client.upsert(table=normalized_table_name, data=normalized_records)

    return normalized_records
