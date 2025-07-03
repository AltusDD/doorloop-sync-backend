import os
import requests
import logging
import traceback

from doorloop_client import fetch_all_records
from supabase_client import upsert_records, ensure_columns_exist

# ✅ Load and validate environment variables
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")

# ✅ Fail fast if environment variables are missing
if not DOORLOOP_API_KEY:
    raise EnvironmentError("❌ Missing required environment variable: DOORLOOP_API_KEY")
if not DOORLOOP_API_BASE_URL:
    raise EnvironmentError("❌ Missing required environment variable: DOORLOOP_API_BASE_URL")

# ✅ Ensure base URL ends with a slash
if not DOORLOOP_API_BASE_URL.endswith("/"):
    DOORLOOP_API_BASE_URL += "/"

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}

# ✅ Enable logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ✅ Endpoints to sync
ENDPOINTS = [
    "accounts", "users", "properties", "units", "leases", "tenants",
    "lease-payments", "lease-returned-payments", "lease-charges", "lease-credits",
    "portfolios", "tasks", "owners", "vendors", "expenses",
    "vendor-bills", "vendor-credits", "communications", "notes", "files"
]

def test_connection():
    test_url = f"{DOORLOOP_API_BASE_URL}accounts"
    try:
        res = requests.get(test_url, headers=HEADERS)
        logging.info(f"✅ Test request to {test_url} returned {res.status_code}")
        if res.status_code == 401:
            logging.error("❌ Unauthorized: Check DOORLOOP_API_KEY")
            logging.error(res.text)
            return False
        if res.status_code >= 400:
            logging.error(f"❌ Error response: {res.status_code} — {res.text}")
            return False
        return True
    except Exception as e:
        logging.error(f"❌ Connection test failed: {e}")
        return False

def main():
    logging.info("✅ 🚀 Starting DoorLoop → Supabase sync")

    if not test_connection():
        logging.error("❌ Aborting sync due to failed connection.")
        return

    for endpoint in ENDPOINTS:
        logging.info(f"✅ 🔄 Syncing endpoint: {endpoint}")
        try:
            records = fetch_all_records(endpoint, DOORLOOP_API_BASE_URL, HEADERS)
            logging.info(f"✅ Retrieved {len(records)} records from {endpoint}")

            if records:
                table_name = endpoint.replace("-", "_")
                ensure_columns_exist(table_name, records)
                upsert_records(table_name, records)

        except Exception as e:
            logging.error(f"❌ ❌ Failed syncing {endpoint}: {e}")
            traceback.print_exc()

    logging.info("✅ 🎉 Sync complete")

if __name__ == "__main__":
    main()
