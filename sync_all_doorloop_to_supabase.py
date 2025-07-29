import os
import datetime
import logging
import uuid
from supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

# --- Set up Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Environment Variables ---
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
    logging.error("Missing DOORLOOP_API_KEY or DOORLOOP_API_BASE_URL environment variables.")
    exit(1)
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

# --- Clients Initialization ---
doorloop_client = DoorLoopClient(DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY)
supabase_ingest_client = SupabaseIngestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# --- API Endpoints and Corresponding Raw Supabase Tables ---
# Ensure these endpoint names match the DoorLoop API documentation (plural, hyphenated where applicable) 
ENDPOINTS = {
    "properties": "properties",
    "units": "units",
    "leases": "leases",
    "tenants": "tenants",
    "owners": "owners",
    "lease-payments": "lease_payments", # Uses underscore for table name
    "lease-charges": "lease_charges",
    "lease-credits": "lease_credits",
    "vendors": "vendors",
    "tasks": "tasks",
    "expenses": "expenses",
    "vendor-bills": "vendor_bills",
    "vendor-credits": "vendor_credits",
    "communications": "communications",
    "notes": "notes",
    "files": "files",
    "property-groups": "property_groups", # Example of specific DoorLoop endpoint name
    # Add other endpoints as needed, ensuring correct pluralization/hyphenation
}

def get_current_timestamp():
    # Use timezone-aware objects to represent datetimes in UTC
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def main_sync_run():
    batch_id = str(uuid.uuid4())
    logging.info(f"Starting sync run with batch ID: {batch_id}")
    
    # Log audit entry for the start of the overall sync
    supabase_ingest_client.log_audit(
        batch_id=batch_id,
        status='in_progress',
        entity='sync_all',
        message='Begin sync run',
        timestamp=get_current_timestamp(),
        entity_type='sync'
    )

    all_sync_succeeded = True

    for api_endpoint, raw_table_suffix in ENDPOINTS.items():
        raw_table_name = f"doorloop_raw_{raw_table_suffix}"
        
        logging.info(f"Syncing {api_endpoint} → {raw_table_name}")
        
        try:
            # Fetch data from DoorLoop API
            data = doorloop_client.fetch_all(api_endpoint)
            
            # De-duplicate records based on 'id' to prevent upsert errors from bad pagination
            unique_records = {item['id']: item for item in data}.values()
            
            # Insert into Supabase raw table
            records_to_insert = [
                {
                    "id": record.get("id"),
                    "data": record,
                    "source_endpoint": api_endpoint,
                    "inserted_at": get_current_timestamp()
                } for record in unique_records
            ]
            
            if records_to_insert:
                supabase_ingest_client.upsert_raw_data(raw_table_name, records_to_insert)
                logging.info(f"✅ Successfully synced {len(records_to_insert)} records for {api_endpoint}.")
            else:
                logging.info(f"ℹ️ No records returned from {api_endpoint}. Skipping insert.")

            # Log audit entry for successful sync of this entity
            supabase_ingest_client.log_audit(
                batch_id=batch_id,
                status='succeeded',
                entity=api_endpoint,
                message=f"Successfully synced {len(records_to_insert)} records.",
                timestamp=get_current_timestamp(),
                entity_type='sync'
            )

        except Exception as e:
            all_sync_succeeded = False
            logging.error(f"Sync failed for {api_endpoint}: {e}")
            # Log audit entry for failed sync of this entity
            supabase_ingest_client.log_audit(
                batch_id=batch_id,
                status='failed',
                entity=api_endpoint,
                message=str(e),
                timestamp=get_current_timestamp(),
                entity_type='sync'
            )
            continue

    # Log audit entry for the end of the overall sync
    final_status = 'complete' if all_sync_succeeded else 'failed'
    final_message = 'All endpoint syncs finished' if all_sync_succeeded else 'One or more endpoint syncs failed'
    
    supabase_ingest_client.log_audit(
        batch_id=batch_id,
        status=final_status,
        entity='sync_all',
        message=final_message,
        timestamp=get_current_timestamp(),
        entity_type='sync'
    )
    logging.info(f"Final Sync Status for batch {batch_id}: {final_status.upper()}")

if __name__ == "__main__":
    main_sync_run()