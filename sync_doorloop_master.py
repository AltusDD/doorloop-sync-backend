from doorloop_client import fetch_data_from_doorloop
from supabase_client import upsert_data_to_supabase

DOORLOOP_ENDPOINTS = [
    "/properties",
    "/units",
    "/tenants",
    "/owners",
    "/leases",
    "/lease-payments",
    "/lease-charges",
    "/lease-credits",
    "/vendors",
    "/tasks",
    "/files",
    "/notes",
    "/communications"
]

if __name__ == "__main__":
    print("\n--- Starting Master DoorLoop Data Sync ---")
    for endpoint in DOORLOOP_ENDPOINTS:
        print(f"🔄 Processing endpoint: {endpoint}")
        data = fetch_data_from_doorloop(endpoint)
        print(f"✅ Fetched {len(data)} records from DoorLoop for {endpoint}.")
        upsert_data_to_supabase(endpoint, data)
    print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
    print("Next: Normalization into core business tables and KPI calculation.")