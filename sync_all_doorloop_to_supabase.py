import os
import logging
import uuid
from datetime import datetime
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOORLOOP_API_KEY = os.environ["DOORLOOP_API_KEY"]
DOORLOOP_API_BASE_URL = os.environ["DOORLOOP_API_BASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]

BATCH_ID = str(uuid.uuid4())
TIMESTAMP = datetime.utcnow().isoformat()

dl_client = DoorLoopClient(base_url=DOORLOOP_API_BASE_URL, api_key=DOORLOOP_API_KEY)
sb_client = SupabaseIngestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def run_sync():
    logger.info(f"Starting sync run with batch ID: {BATCH_ID}")
    sb_client.log_audit(batch_id=BATCH_ID, status="in_progress", entity="sync_all", message="Begin sync run")

    endpoints = {
        "properties": "doorloop_raw_properties",
        "units": "doorloop_raw_units",
        "leases": "doorloop_raw_leases",
        "tenants": "doorloop_raw_tenants",
        "owners": "doorloop_raw_owners",
        "lease-payments": "doorloop_raw_lease_payments",
        "lease-charges": "doorloop_raw_lease_charges",
        "lease-credits": "doorloop_raw_lease_credits",
        "vendors": "doorloop_raw_vendors",
        "tasks": "doorloop_raw_tasks",
        "expenses": "doorloop_raw_expenses",
        "vendor-bills": "doorloop_raw_vendor_bills",
        "vendor-credits": "doorloop_raw_vendor_credits",
        "communications": "doorloop_raw_communications",
        "notes": "doorloop_raw_notes",
        "files": "doorloop_raw_files",
        "property-groups": "doorloop_raw_property_groups"
    }

    for endpoint, table in endpoints.items():
        logger.info(f"Syncing {endpoint} → {table}")
        try:
            records = dl_client.fetch_all(endpoint)
            sb_client.store_raw_data(table_name=table, records=records, batch_id=BATCH_ID)
        except Exception as e:
            logger.error(f"Sync failed for {endpoint}: {e}")
            sb_client.log_audit(batch_id=BATCH_ID, status="failed", entity=endpoint, message=str(e))

    sb_client.log_audit(batch_id=BATCH_ID, status="complete", entity="sync_all", message="All endpoint syncs finished")

if __name__ == "__main__":
    run_sync()
