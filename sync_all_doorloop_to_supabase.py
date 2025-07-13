import logging
import os
import time
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient
from supabase_schema_manager import SupabaseSchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/v1")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([DOORLOOP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    raise EnvironmentError("❌ Required environment variables missing. Check API keys and Supabase config.")

# Initialize clients
dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
sb_client = SupabaseIngestClient(supabase_url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)
schema_manager = SupabaseSchemaManager(supabase_url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)

# Define endpoints to sync → destination raw tables
SYNC_TARGETS = {
    "accounts": "doorloop_raw_accounts",
    "users": "doorloop_raw_users",
    "properties": "doorloop_raw_properties",
    "units": "doorloop_raw_units",
    "leases": "doorloop_raw_leases",
    "tenants": "doorloop_raw_tenants",
    "lease-payments": "doorloop_raw_lease_payments",
    "lease-charges": "doorloop_raw_lease_charges",
    "lease-credits": "doorloop_raw_lease_credits",
    "tasks": "doorloop_raw_tasks",
    "owners": "doorloop_raw_owners",
    "vendors": "doorloop_raw_vendors",
    "expenses": "doorloop_raw_expenses",
    "vendor-bills": "doorloop_raw_vendor_bills",
    "vendor-credits": "doorloop_raw_vendor_credits",
    "communications": "doorloop_raw_communications",
    "notes": "doorloop_raw_notes",
    "files": "doorloop_raw_files",
    "property-groups": "doorloop_raw_portfolios",
    "lease-reversed-payments": "doorloop_raw_lease_reversed_payments",
}

# Optional delay to allow Supabase schema cache to refresh
SCHEMA_CACHE_DELAY = int(os.getenv("SCHEMA_CACHE_DELAY", "5"))

# Run full sync process
for endpoint, table in SYNC_TARGETS.items():
    try:
        logger.info(f"🔄 Syncing {endpoint} into {table}...")
        records = dl_client.fetch_all(endpoint)

        if not records:
            logger.warning(f"⚠️ No records returned from {endpoint}")
            continue

        # Create or update the raw table schema
        schema_manager.ensure_raw_table_exists(table)
        schema_manager.add_missing_columns(table, records)

        logger.info(f"🔁 PostgREST schema refresh delay: {SCHEMA_CACHE_DELAY}s")
        time.sleep(SCHEMA_CACHE_DELAY)

        # Insert or update the data
        sb_client.upsert_data(table, records)

    except Exception as e:
        logger.exception(f"🔥 Failed to sync {endpoint} → {table}: {str(e)}")
