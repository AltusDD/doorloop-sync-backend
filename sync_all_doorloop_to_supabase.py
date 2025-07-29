import os
print("🛠️ DEBUG – Effective BASE URL:", os.environ.get("DOORLOOP_API_BASE_URL"))
import logging
import uuid
import time
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient
from supabase_schema_manager import SupabaseSchemaManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/v1")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([DOORLOOP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    raise EnvironmentError("Missing required environment variables")

dl_client = DoorLoopClient()
sb_client = SupabaseIngestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
schema_mgr = SupabaseSchemaManager(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

ENDPOINTS = {
    "properties": "doorloop_raw_properties",
    "units": "doorloop_raw_units",
    "leases": "doorloop_raw_leases",
    "tenants": "doorloop_raw_tenants",
    "owners": "doorloop_raw_owners",
    "lease-payments": "doorloop_raw_lease_payments",
}

def run_sync():
    batch_id = str(uuid.uuid4())
    logger.info(f"Starting sync run with batch ID: {batch_id}")
    sb_client.log_audit(batch_id, "in_progress", "sync_all", "Begin sync run")

    for endpoint, table in ENDPOINTS.items():
        try:
            logger.info(f"Syncing {endpoint} → {table}")
            records = dl_client.fetch_all(endpoint)
            if not records:
                logger.warning(f"No data from {endpoint}")
                sb_client.log_audit(batch_id, "warning", endpoint, "No records returned")
                continue

            schema_mgr.ensure_raw_table_exists(table)
            schema_mgr.add_missing_columns(table, records)
            time.sleep(3)

            sb_client.upsert_data(table, records, on_conflict="doorloop_id")
            sb_client.log_audit(batch_id, "success", endpoint, f"Upserted {len(records)}", len(records))
        except Exception as e:
            logger.exception(f"Sync failed for {endpoint}: {e}")
            sb_client.log_audit(batch_id, "failed", endpoint, str(e))

    sb_client.log_audit(batch_id, "complete", "sync_all", "All endpoint syncs finished")

if __name__ == "__main__":
    run_sync()
