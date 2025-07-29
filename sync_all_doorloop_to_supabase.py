import os
import logging
import uuid
from datetime import datetime, timezone
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

dl_client = DoorLoopClient()
sb_client = SupabaseIngestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

BATCH_ID = str(uuid.uuid4())
TIMESTAMP = datetime.now(timezone.utc).isoformat()

def run_sync():
    logger.info(f"Starting sync run with batch ID: {BATCH_ID}")
    sb_client.log_audit(
        batch_id=BATCH_ID,
        status="in_progress",
        entity="sync_all",
        message="Begin sync run",
        timestamp=TIMESTAMP,
    )

    endpoints = {
        "property": "doorloop_raw_properties",
        "unit": "doorloop_raw_units",
        "lease": "doorloop_raw_leases",
        "tenant": "doorloop_raw_tenants",
        "owner": "doorloop_raw_owners",
        "leasePayment": "doorloop_raw_lease_payments",
    }

    for endpoint, table in endpoints.items():
        try:
            logger.info(f"Syncing {endpoint} → {table}")
            records = dl_client.fetch_all(endpoint)
            sb_client.insert_raw(table, records, batch_id=BATCH_ID)
        except Exception as e:
            logger.error(f"Sync failed for {endpoint}: {e}")
            sb_client.log_audit(
                batch_id=BATCH_ID,
                status="failed",
                entity=endpoint,
                message=str(e),
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

    sb_client.log_audit(
        batch_id=BATCH_ID,
        status="complete",
        entity="sync_all",
        message="All endpoint syncs finished",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

if __name__ == "__main__":
    run_sync()
