import os
import requests
from doorloop_client import fetch_all_records
from supabase_client import upsert_records

# ✅ Fixed: Ensure base URL is correct with fallback
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/api/")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}

def main():
    print("--- Starting Master DoorLoop Data Sync ---")

    # ✅ Test connection using base URL
    test_url = f"{DOORLOOP_API_BASE_URL}accounts"
    print(f"👋 Testing connection to: {test_url}")
    try:
        res = requests.get(test_url, headers=HEADERS)
        print("👋 Test Connection Status:", res.status_code)
        if res.status_code == 200:
            print("👋 Test Connection: Successful!")
        elif res.status_code == 401:
            print("🔐 Unauthorized: API Key may be invalid.")
            print(f"Raw response: {res.text}")
            exit(1)
        else:
            print(f"👋 Test Connection: Failed with status code {res.status_code}.")
            print(f"Raw response: {res.text}")
            exit(1)
    except requests.exceptions.RequestException as e:
        print(f"❌ Test Connection: Exception occurred - {e}")
        exit(1)

    # ✅ Proceed with syncing all endpoints
    endpoints = [
        "properties", "units", "tenants", "owners",
        "leases", "lease-payments", "lease-charges", "lease-credits",
        "vendors", "tasks", "files", "notes", "communications"
    ]

    for endpoint in endpoints:
        print(f"🔄 Syncing /{endpoint} ...")
        try:
            records = fetch_all_records(endpoint)
            print(f"✅ Retrieved {len(records)} records.")
            table_name = endpoint.replace("-", "_")
            upsert_records(table_name, records)
        except Exception as e:
            print(f"❌ Failed to process endpoint /{endpoint}: {e}")

    print("--- ✅ Master Sync Complete ---")

if __name__ == "__main__":
    main()
