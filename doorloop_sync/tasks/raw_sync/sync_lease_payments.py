import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.utils.data_processing import standardize_records

logger = logging.getLogger(__name__)

def sync_lease_payments():
    logger.info("Starting raw sync for lease_payments...")
    doorloop = DoorLoopClient()
    supabase = SupabaseClient()

    table_name = 'doorloop_raw_lease_payments'
    endpoint = 'lease-payments'

    try:
        all_records = doorloop.get_all(endpoint)

        if not all_records:
            logger.info("No records found for lease_payments.")
            return

        logger.info(f"Fetched {len(all_records)} total records for lease_payments.")
        standardized_records = standardize_records(all_records)

        if not standardized_records:
            logger.warning("No valid dictionary records found in payload to standardize.")
            logger.info(f"⏭️ Skipping upsert to '{table_name}' — no valid records after standardization.")
            return

        supabase.upsert(table_name, standardized_records)
        logger.info(f"Successfully upserted {len(standardized_records)} records to {table_name}.")

    except Exception as e:
        logger.error(f"An error occurred during raw sync for lease_payments: {e}", exc_info=True)
        raise
