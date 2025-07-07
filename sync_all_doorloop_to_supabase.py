# sync_all_doorloop_to_supabase.py

import logging
import time
from doorloop_client import DoorLoopClient
from supabase_ingest_client import upsert_data

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get DoorLoop API details from environment variables (must be set in Azure/GitHub)
import os
DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
    logger.error("❌ Environment variables DOORLOOP_API_KEY and/or DOORLOOP_API_BASE_URL not set.")
    exit(1)

dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)

# All endpoints to sync
SYNC_ENDPOINTS = [
    "accounts",
    "users",
    "properties",
    "units",
    "leases",
    "tenants",
    "lease-payments",
    "lease-charges",
    "lease-credits",
    "lease-reversed-payments",
    "owners",
    "vendors",
    "tasks",
    "expenses",
    "vendor-bills",
    "vendor-credits",
    "communications",
    "notes",
    "files",
    "property-groups"
]

for endpoint in SYNC_ENDPOINTS:
    table_name = f"doorloop_raw_{endpoint.replace('-', '_')}"
    try:
        logger.info(f"🔄 Syncing {endpoint} into {table_name}...")

        # Ensure table exists (created dynamically in ingest)
        records = dl_client.fetch_all(endpoint)
        if not records:
            logger.warning(f"⚠️ No records returned from {endpoint}")
            continue

        # Write data
        upsert_data(table_name, records)

        # PostgREST schema refresh
        logger.info("🔁 PostgREST schema refresh delay: 5s")
        time.sleep(5)

    except Exception as e:
        logger.error(f"🔥 Failed to sync {endpoint} → {table_name}: {e}")
