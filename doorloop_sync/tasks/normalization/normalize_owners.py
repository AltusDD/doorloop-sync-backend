from decimal import Decimal
from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

logger = get_logger(__name__)

@task_error_handler
def run():
    logger.info("Starting normalization for owners...")
    supabase = get_supabase_client()
    
    raw_table_name = "doorloop_raw_owners"
    normalized_table_name = "doorloop_normalized_owners"

    raw_records = supabase.fetch_all(raw_table_name)

    if not raw_records:
        logger.info(f"No raw data in {raw_table_name} to normalize.")
        return

    normalized_records = []
    for record in raw_records:
        normalized_data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
            "type": record.get("type"),
            "balance": record.get("balance"),
        }

        # FIX: Robustly clean the record to remove any keys that have a None value.
        # This prevents errors if the table has NOT NULL constraints.
        final_record = {k: v for k, v in normalized_data.items() if v is not None}
        
        # Special handling for balance to ensure it's a float
        if 'balance' in final_record:
            try:
                final_record['balance'] = float(Decimal(final_record['balance']))
            except (ValueError, TypeError):
                logger.warning(f"Could not cast balance for owner {final_record.get('doorloop_id')}. Removing field.")
                del final_record['balance']

        normalized_records.append(final_record)

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table_name}...")
        supabase.upsert(table=normalized_table_name, data=normalized_records)
