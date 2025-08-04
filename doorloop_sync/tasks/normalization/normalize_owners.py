from decimal import Decimal
from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler

logger = get_logger(__name__)

def clean_and_cast_record(record: dict) -> dict:
    """
    Cleans and casts data types for a record before upserting.
    - Removes keys where the value is None.
    - Casts 'balance' to a float if it exists.
    """
    # Cast balance to float if it's not None
    if record.get('balance') is not None:
        try:
            record['balance'] = float(Decimal(record['balance']))
        except (ValueError, TypeError):
            logger.warning(f"Could not cast balance '{record['balance']}' to float. Setting to None.")
            record['balance'] = None
            
    # Return a new dict with None values removed
    return {k: v for k, v in record.items() if v is not None}

@task_error_handler
def run():
    """
    Normalizes raw owner data and upserts it into the
    doorloop_normalized_owners table.
    """
    logger.info("Starting normalization for owners...")
    supabase = get_supabase_client()
    
    raw_table_name = "doorloop_raw_owners"
    normalized_table_name = "doorloop_normalized_owners"

    raw_records = supabase.fetch_all(raw_table_name)

    if not raw_records:
        logger.info(f"No raw data in {raw_table_name} to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        normalized_data = {
            "doorloop_id": record.get("id"),
            "name": record.get("name"),
            "type": record.get("type"),
            "balance": record.get("balance"),
        }
        # FIX: Use a more robust cleaning and casting function.
        normalized_records.append(clean_and_cast_record(normalized_data))

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table_name}...")
        supabase.upsert(table=normalized_table_name, data=normalized_records)
    else:
        logger.info("No records to upsert after normalization.")
