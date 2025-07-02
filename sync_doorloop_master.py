import os
from doorloop_client import fetch_all_records
from supabase_client import upsert_records
import requests

DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Accept": "application/json"
}

def main():
    print("--- Starting Master DoorLoop Data Sync ---")

    # Initial test connection
    test_url = f"{DOORLOOP_API_BASE_URL}accounts"
    try:
        res = requests.get(test_url, headers=HEADERS)
        print("👋 Test Connection Status:", res.status_code)
        if res.status_code == 200:
            print("👋 Test Connection: Successful!")
        else:
            print(f"👋 Test Connection: Failed with status code {res.status_code}. Raw response: {res.text}")
            exit(1)
    except requests.exceptions.RequestException as e:
        print(f"👋 Test Connection: Could not connect to API. Error: {e}")
        exit(1)

    endpoints = [
        "properties", "units", "tenants", "owners",
        "leases", "lease-payments", "lease-charges", "lease-credits",
        "vendors", "tasks", "files", "notes", "communications"
    ]

    for endpoint in endpoints:
        print(f"🔄 Processing endpoint: /{endpoint}")
        records = fetch_all_records(endpoint)
        print(f"✅ Fetched {len(records)} records from DoorLoop for /{endpoint}.")
        table_name = endpoint.replace("-", "_")
        upsert_records(table_name, records)

    print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
    print("Next: Normalization into core business tables and KPI calculation.")

if __name__ == "__main__":
    main()
