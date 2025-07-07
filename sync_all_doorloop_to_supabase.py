# sync_all_doorloop_to_supabase.py

import os
import logging
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load secrets from environment
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Ensure all required environment variables are set
missing = []
if not DOORLOOP_API_KEY: missing.append("DOORLOOP_API_KEY")
if not DOORLOOP_API_BASE_URL: missing.append("DOORLOOP_API_BASE_URL")
if not SUPABASE_URL: missing.append("SUPABASE_URL")
if not SUPABASE_API_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
if missing:
    raise EnvironmentError(f"Missing required environment variable(s): {', '.join(missing)}")

# Define modules to sync
DOORLOOP_MODULES = [
    "accounts", "users", "properties", "units", "leases", "tenants",
    "lease-payments", "lease-charges", "lease-credits", "lease-reversed-payments",
    "owners", "vendors", "tasks", "expenses", "vendor-bills", "vendor-credits",
    "communications", "notes", "files", "property-groups"
]

if __name__ == "__main__":
    logger.info("🚀 Starting DoorLoop → Supabase sync")

    dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
    sb_client = SupabaseIngestClient(url=SUPABASE_URL, api_key=SUPABASE_API_KEY)

    for module in DOORLOOP_MODULES:
        table = f"doorloop_raw_{module.replace('-', '_')}"
        logger.info(f"🔄 Syncing {module} into {table}...")
        try:
            records = dl_client.get_all_records(module)
            sb_client.upsert_data(table, records)
        except Exception as e:
            logger.error(f"🔥 Failed to sync {module} → {table}: {e}")
