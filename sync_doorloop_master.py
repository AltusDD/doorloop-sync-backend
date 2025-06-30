# sync_doorloop_master.py
import os
from dotenv import load_dotenv
# --- FIX: Correct the function name being imported ---
from doorloop_client import fetch_all_doorloop_records 
from supabase_client import upsert_raw_doorloop_data  # Corrected function name

# Load environment variables from .env file (if running locally)
load_dotenv()

# === CONFIGURATION ===
# Ensure these environment variables are set in your execution environment (e.g., .env, GitHub Actions secrets)
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://app.doorloop.com/api")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") # For direct psycopg2 connection (if used in other parts)

# Define all DoorLoop endpoints and their corresponding raw Supabase table names
# The key is the API endpoint, the value is the base name for doorloop_raw_<name> table.
ALL_DOORLOOP_ENDPOINTS = [
    "/properties", "/units", "/tenants", "/owners", "/leases", "/lease-payments",
    "/lease-charges", "/lease-credits", "/vendors", "/tasks", "/files", "/notes",
    "/communications", "/applications", "/inspections", "/insurance-policies",
    "/recurring-charges", "/recurring-credits", "/accounts", "/users",
    "/portfolios", "/reports", "/activity-logs",
    # Add any other specific endpoints from your OpenAPI spec if missed
]

# === MAIN SYNC ORCHESTRATION ===
async def run_master_sync():
    print("--- Starting Master DoorLoop Data Sync ---")

    # --- Environment Variable Check ---
    if not all([DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
        missing_vars = [name for name, val in {
            "DOORLOOP_API_BASE_URL": DOORLOOP_API_BASE_URL,
            "DOORLOOP_API_KEY": DOORLOOP_API_KEY,
            "SUPABASE_URL": SUPABASE_URL,
            "SUPABASE_SERVICE_ROLE_KEY": SUPABASE_SERVICE_ROLE_KEY
        }.items() if not val]
        print(f"❌ CRITICAL ERROR: Missing environment variables: {', '.join(missing_vars)}. Sync cannot proceed.")
        return # Exit if critical env vars are missing

    print(f"🔑 Env vars SET. Base URL: {DOORLOOP_API_BASE_URL}")

    for endpoint in ALL_DOORLOOP_ENDPOINTS:
        print(f"\n🔄 Processing endpoint: {endpoint}")
        try:
            # --- Step 1: Fetch from DoorLoop API ---
            records = fetch_all_doorloop_records(endpoint, DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY)
            print(f"✅ Fetched {len(records)} records from DoorLoop for {endpoint}.")

            # --- Step 2: Upsert into Supabase Raw Tables (raw_doorloop_data & doorloop_raw_<entity>) ---
            if records: # Only attempt upsert if records were fetched
                upsert_raw_doorloop_data(endpoint, records, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                print(f"✅ Successfully upserted {len(records)} records to Supabase raw tables for {endpoint}.")
            else:
                print(f"⚠️ No records fetched for {endpoint}. Skipping Supabase upsert.")

        except Exception as e:
            print(f"❌ Failed to sync {endpoint}: {type(e).__name__}: {str(e)}")
            # Optionally log to a sync_errors table here for persistent audit
            continue # Continue to next endpoint even if one fails

    print("\n--- Master Sync Orchestration Complete (Raw Ingestion Phase). ---")
    print("Next: Normalization into core business tables and KPI calculation.")

# === ENTRY POINT ===
if __name__ == "__main__":
    import asyncio
    asyncio.run(run_master_sync())