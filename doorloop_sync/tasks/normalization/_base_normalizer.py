from doorloop_sync.config import get_supabase_client, get_logger
from doorloop_sync.utils.decorators import task_error_handler
from decimal import Decimal

logger = get_logger(__name__)

@task_error_handler
def run():
    """
    Normalizes raw _base_normalizer data from Supabase and upserts it into the
    doorloop_normalized__base_normalizer table.
    """
    entity_name = "_base_normalizer"
    raw_table = f"doorloop_raw_{entity_name}"
    normalized_table = f"doorloop_normalized_{entity_name}"

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
            "type": record.get("accountType"),
            "balance": record.get("balance"),
        }

        final_record = {k: v for k, v in normalized_data.items() if v is not None}

        if 'balance' in final_record:
            try:
                final_record['balance'] = float(Decimal(final_record['balance']))
            except (ValueError, TypeError):
                logger.warning(f"Could not cast balance for account {final_record.get('doorloop_id')}. Removing field.")
                del final_record['balance']

        if final_record:
            normalized_records.append(final_record)

    if normalized_records:
        logger.info(f"Upserting {len(normalized_records)} normalized records to {normalized_table}...")
        supabase.upsert(table=normalized_table, data=normalized_records)
    else:
        logger.info("No valid records to upsert after normalization.")
