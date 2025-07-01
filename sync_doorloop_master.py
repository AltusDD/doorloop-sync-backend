
import os
from dotenv import load_dotenv
from doorloop_client import fetch_all_entities
from supabase_client import upsert_raw_doorloop_data

load_dotenv()

DOORLOOP_ENDPOINTS = [
    "properties",
    "units",
    "tenants",
    "owners",
    "leases",
    "lease-payments",
    "lease-charges",
    "lease-credits",
    "tasks",
    "work-orders",
    "attachments",
    "communications",
    "accounts",
    "users",
    "portfolios",
    "expenses",
    "vendor-bills",
    "vendor-credits",
    "recurring-charges",
    "recurring-credits",
    "recurring-payments",
    "applications"
]

def main():
    print("--- Starting Master DoorLoop Data Sync ---")

    print(f"🔑 Env vars SET. Base URL: [{os.getenv('DOORLOOP_API_BASE_URL', 'Default')}]")

    for endpoint in DOORLOOP_ENDPOINTS:
        print(f"\n🔄 Processing endpoint: /{endpoint}")
        try:
            data = fetch_all_entities(endpoint)
            if data is None or not isinstance(data, list):
                raise Exception("Returned data is not a list")

            print(f"✅ Fetched {len(data)} records from DoorLoop for /{endpoint}.")
            upsert_raw_doorloop_data(endpoint, data)
        except Exception as e:
            print(f"❌ Failed to sync /{endpoint}: {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()
