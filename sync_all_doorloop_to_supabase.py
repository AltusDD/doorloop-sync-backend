# sync_all_doorloop_to_supabase.py

import os
import logging
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load API credentials from environment
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL:
    raise ValueError("Missing DOORLOOP_API_KEY or DOORLOOP_API_BASE_URL environment variables.")

# List of DoorLoop modules to sync
DOORLOOP_MODULES = [
    "accounts", "users", "properties", "units", "leases", "tenants",
    "lease-payments", "lease-charges", "lease-credits", "lease-reversed-payments",
    "owners", "vendors", "tasks", "expenses", "vendor-bills", "vendor-credits",
    "communications", "notes", "files", "property-groups"
]

if __name__ == "__main__":
    logger.info("🚀 Starting DoorLoop → Supabase sync")

    dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
    sb_client = SupabaseIngestClient()

    for module in DOORLOOP_MODULES:
        logger.info(f"🔄 Syncing {module} into doorloop_raw_{module.replace('-', '_')}...")
        try:
            records = dl_client.get_all_records(module)
            sb_client.upsert_data(f"doorloop_raw_{module.replace('-', '_')}", records)
        except Exception as e:
            logger.error(f"🔥 Failed to sync {module} → doorloop_raw_{module.replace('-', '_')}: {e}")
