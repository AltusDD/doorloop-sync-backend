
# sync_doorloop_master.py
import os
from dotenv import load_dotenv
from doorloop_client import fetch_all_records
from supabase_client import upsert_records

load_dotenv()

DOORLOOP_ENDPOINTS = [
    "properties", "units", "tenants", "owners", "leases",
    "lease-payments", "lease-charges", "lease-credits",
    "vendors", "tasks", "files", "notes", "communications"
]

def normalize_table_name(endpoint):
    return "doorloop_raw_" + endpoint.replace("-", "_")

def main():
    print("\n--- Starting Master DoorLoop Data Sync ---")
    for endpoint in DOORLOOP_ENDPOINTS:
        print(f"🔄 Processing endpoint: /{endpoint}")
        records = fetch_all_records(endpoint)
        print(f"✅ Fetched {len(records)} records from DoorLoop for /{endpoint}.")

        if records:
            table_name = normalize_table_name(endpoint)
            print(f"📤 Upserting {len(records)} records to Supabase table: {table_name}")
            upsert_records(table_name, records)
    print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
    print("Next: Normalization into core business tables and KPI calculation.\n")

if __name__ == "__main__":
    main()
