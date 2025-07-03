import logging
import time
import doorloop_client
import supabase_client

# Setup logging
LOG_FILE = "doorloop_sync.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
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

    for endpoint in ENDPOINTS:
        try:
            log_success(f"🔄 Syncing endpoint: {endpoint}")
            records = doorloop_client.fetch_all(endpoint)
            if not records:
                log_success(f"No data for {endpoint}. Skipping.")
                continue

            table_name = endpoint.replace("-", "_")
            if endpoint == "portfolios":
                table_name = "property_groups"

            supabase_client.upsert_data(
                table_name=table_name,
                records=records,
                primary_key_field="id"
            )

            log_success(f"✅ Synced {len(records)} records to {table_name}")
        except Exception as e:
            log_error(f"❌ Failed syncing {endpoint}: {e}")

    log_success(f"🎉 Sync complete in {time.time() - start:.2f}s")
