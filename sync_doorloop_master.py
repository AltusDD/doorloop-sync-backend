
import os
from doorloop_client import fetch_data_from_doorloop
from supabase_client import post_to_supabase

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL")

ENDPOINTS = [
    "properties", "units", "tenants", "owners", "leases",
    "lease-payments", "lease-charges", "lease-credits",
    "vendors", "tasks", "files", "notes", "communications"
]

print("--- Starting Master DoorLoop Data Sync ---")
print(f"🔑 Env vars SET. Base URL: {DOORLOOP_BASE_URL}")

for endpoint in ENDPOINTS:
    try:
        print(f"🔄 Processing endpoint: /{endpoint}")
        data = fetch_data_from_doorloop(endpoint, DOORLOOP_API_KEY, DOORLOOP_BASE_URL)
        print(f"✅ Fetched {len(data)} records from DoorLoop for /{endpoint}.")
        post_to_supabase("raw_doorloop_data", data)
        print(f"✅ Synced /{endpoint} to Supabase.")
    except Exception as e:
        print(f"❌ Failed to sync /{endpoint}: {type(e).__name__}: {e}")

print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
print("Next: Normalization into core business tables and KPI calculation.")
