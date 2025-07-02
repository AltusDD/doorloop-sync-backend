
import os
from doorloop_client import fetch_data_from_doorloop
from supabase_client import post_to_supabase

endpoints = [
    "/properties", "/units", "/tenants", "/owners", "/leases", "/lease-payments",
    "/lease-charges", "/lease-credits", "/vendors", "/tasks", "/files", "/notes", "/communications"
]

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL")

if not DOORLOOP_API_KEY or not DOORLOOP_BASE_URL:
    print("❌ Environment variables not set:")
    print("DOORLOOP_API_KEY =", DOORLOOP_API_KEY)
    print("DOORLOOP_BASE_URL =", DOORLOOP_BASE_URL)
    raise EnvironmentError("Missing DoorLoop API configuration in environment variables.")

print("--- Starting Master DoorLoop Data Sync ---")
print("🔑 Env vars SET. Base URL:", DOORLOOP_BASE_URL)

for endpoint in endpoints:
    print(f"🔄 Processing endpoint: {endpoint}")
    try:
        data = fetch_data_from_doorloop(endpoint, DOORLOOP_BASE_URL, DOORLOOP_API_KEY)
        print(f"✅ Fetched {len(data)} records from DoorLoop for {endpoint}.")
        post_to_supabase(endpoint, data)
    except Exception as e:
        print(f"❌ Failed to sync {endpoint}: {type(e).__name__}: {str(e)}")

print("--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
print("Next: Normalization into core business tables and KPI calculation.")
