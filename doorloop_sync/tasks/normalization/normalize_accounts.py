
from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.task_error_handler import task_error_handler

logger = get_logger(__name__)

@task_error_handler
def run():
    logger.info("Starting normalization for accounts...")

    supabase = get_supabase_client()
    raw_table = "doorloop_raw_accounts"
    normalized_table = "doorloop_normalized_accounts"

    response = supabase.supabase.table(raw_table).select("data").execute()
    raw_records = response.data or []

    unique = {
        item['data']['id']: item['data']
        for item in raw_records
        if item.get('data') and item['data'].get('id')
    }.values()

    normalized = []
    for r in unique:
        try:
            normalized.append({
                "doorloop_id": r.get("id"),
                "name": r.get("name"),
                "type": r.get("type"),
                "balance": float(r.get("balance", 0)) if r.get("balance") is not None else 0.0
            })
        except Exception as e:
            logger.warning(f"⚠️ Skipping record with error: {e}")

    if normalized:
        supabase.upsert(table=normalized_table, data=normalized)
        logger.info(f"✅ Upserted {len(normalized)} account records.")
