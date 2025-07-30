import os
import datetime
import logging
import uuid

# --- Corrected Imports ---
from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.normalization import run_all_normalizations

# --- Set up Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Environment Variables ---
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


def main_sync_run():
    if not all([DOORLOOP_API_KEY, DOORLOOP_API_BASE_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
        logging.error("Missing one or more required environment variables.")
        exit(1)

    # --- Clients Initialization ---
    doorloop_client = DoorLoopClient(DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY)
    supabase_client = SupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    ENDPOINTS = {
        "properties": "properties", "units": "units", "leases": "leases", "tenants": "tenants",
        "owners": "owners", "lease-payments": "lease_payments", "lease-charges": "lease_charges",
        "lease-credits": "lease_credits", "vendors": "vendors", "tasks": "tasks", "expenses": "expenses",
        "vendor-bills": "vendor_bills", "vendor-credits": "vendor_credits", "communications": "communications",
        "notes": "notes", "files": "files", "property-groups": "property_groups",
    }

    batch_id = str(uuid.uuid4())
    logging.info(f"Starting sync run with batch ID: {batch_id}")

    supabase_client.log_audit(
        batch_id=batch_id, status='in_progress', entity='sync_all',
        message='Begin sync run'
    )

    all_sync_succeeded = True

    for api_endpoint, raw_table_suffix in ENDPOINTS.items():
        raw_table_name = f"doorloop_raw_{raw_table_suffix}"
        logging.info(f"Syncing {api_endpoint} → {raw_table_name}")

        try:
            data = doorloop_client.fetch_all(api_endpoint)
            if not data:
                logging.info(f"ℹ️ No records returned from {api_endpoint}. Skipping insert.")
                supabase_client.log_audit(
                    batch_id=batch_id, status='succeeded', entity=api_endpoint,
                    message='Successfully synced 0 records.'
                )
                continue

            unique_records = list({item['id']: item for item in data}.values())
            records_to_insert = [
                {"id": record.get("id"), "data": record, "source_endpoint": api_endpoint}
                for record in unique_records
            ]

            supabase_client.upsert_raw_data(raw_table_name, records_to_insert)
            logging.info(f"✅ Successfully synced {len(records_to_insert)} records for {api_endpoint}.")

            supabase_client.log_audit(
                batch_id=batch_id, status='succeeded', entity=api_endpoint,
                message=f"Successfully synced {len(records_to_insert)} records."
            )

        except Exception as e:
            all_sync_succeeded = False
            logging.error(f"Sync failed for {api_endpoint}: {e}")
            supabase_client.log_audit(
                batch_id=batch_id, status='failed', entity=api_endpoint,
                message=str(e)
            )
            continue

    final_status = 'complete' if all_sync_succeeded else 'failed'
    final_message = 'All endpoint syncs finished' if all_sync_succeeded else 'One or more endpoint syncs failed'

    supabase_client.log_audit(
        batch_id=batch_id, status=final_status, entity='sync_all',
        message=final_message
    )
    logging.info(f"Final Sync Status for batch {batch_id}: {final_status.upper()}")

    # ✅ Run normalization phase after raw sync
    logging.info("🔁 Starting normalization process...")
    run_all_normalizations(supabase_client)


if __name__ == "__main__":
    main_sync_run()
