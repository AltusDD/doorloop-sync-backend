import logging
import time
import os
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient

LOG_FILE = "doorloop_sync.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)

def log_success(message):
    logging.info("✅ " + message)

def log_error(message):
    logging.error("❌ " + message)

ENDPOINTS = [
    "accounts", "users", "properties", "units", "leases", "tenants", "lease-payments",
    "lease-returned-payments", "lease-charges", "lease-credits", "portfolios", "tasks",
    "owners", "vendors", "expenses", "vendor-bills", "vendor-credits", "communications",
    "notes", "files"
]

if __name__ == "__main__":
    start = time.time()
    log_success("🚀 Starting DoorLoop → Supabase sync")

    try:
        DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
        DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not DOORLOOP_API_KEY or not DOORLOOP_API_BASE_URL or not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError("❌ One or more required environment variables are not set.")

        dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
        sb_client = SupabaseClient(url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)

        for endpoint in ENDPOINTS:
            try:
                log_success(f"🔄 Syncing endpoint: {endpoint}")
                records = dl_client.fetch_all(endpoint)
                if not records:
                    log_success(f"No data for {endpoint}. Skipping.")
                    continue

                table_name = endpoint.replace("-", "_")
                if endpoint == "portfolios":
                    table_name = "property_groups"

                sb_client.upsert_data(
                    table_name=table_name,
                    records=records,
                    primary_key_field="id"
                )
                log_success(f"✅ Synced {len(records)} records to {table_name}")
            except Exception as e:
                log_error(f"❌ Failed syncing {endpoint}: {e}")
                continue

    except ValueError as ve:
        log_error(f"❌ Configuration Error: {ve}")
        exit(1)
    except Exception as e:
        log_error(f"❌ Unexpected error during sync: {e}")
        exit(1)

    log_success(f"🎉 Sync complete in {time.time() - start:.2f}s")