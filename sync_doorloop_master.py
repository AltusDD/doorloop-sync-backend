import os
from doorloop_client import fetch_data_from_doorloop

print("\n--- Starting Master DoorLoop Data Sync ---")
print(f"🔑 Env vars SET. Base URL: {os.environ.get('DOORLOOP_API_BASE_URL')}")

endpoints = [
    "/properties", "/units", "/tenants", "/owners", "/leases",
    "/lease-payments", "/lease-charges", "/lease-credits",
    "/vendors", "/tasks", "/files", "/notes", "/communications"
]

for endpoint in endpoints:
    try:
        print(f"🔄 Processing endpoint: {endpoint}")
        data = fetch_data_from_doorloop(endpoint)
        print(f"✅ Fetched {len(data)} records from DoorLoop for {endpoint}.")
        # Placeholder for upsert logic
    except Exception as e:
        print(f"❌ Failed to sync {endpoint}: {type(e).__name__}: {e}")

print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
print("Next: Normalization into core business tables and KPI calculation.")