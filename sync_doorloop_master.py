
from doorloop_client import fetch_data_from_doorloop

endpoints = [
    "/properties", "/units", "/tenants", "/owners", "/leases",
    "/lease-payments", "/lease-charges", "/lease-credits",
    "/vendors", "/tasks", "/files", "/notes", "/communications"
]

print("--- Starting Master DoorLoop Data Sync ---")

for endpoint in endpoints:
    try:
        print(f"🔄 Processing endpoint: {endpoint}")
        data = fetch_data_from_doorloop(endpoint)
        print(f"✅ Fetched {len(data)} records from DoorLoop for {endpoint}.")
        # Placeholder for upsert call: upsert_data(endpoint, data)
    except Exception as e:
        print(f"❌ Failed to sync {endpoint}: {e}")

print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
print("Next: Normalization into core business tables and KPI calculation.")
