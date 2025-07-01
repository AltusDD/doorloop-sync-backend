import os
from dotenv import load_dotenv
from doorloop_client import fetch_all_entities
from supabase_client import upsert_raw_doorloop_data

load_dotenv()

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Master list of endpoints to sync
ENDPOINTS = [
    "/properties",
    "/units",
    "/tenants",
    "/owners",
    "/leases",
    "/lease-payments",
    "/lease-charges",
    "/lease-credits"
]

def get_table_name(endpoint: str) -> str:
    # Convert endpoint to table name format
    return f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"

async def run_master_sync():
    print(f"--- Starting Master DoorLoop Data Sync ---")
    print(f"🔑 Env vars SET. Base URL: {os.getenv('DOORLOOP_BASE_URL', '[Default]')}")

    for endpoint in ENDPOINTS:
        print(f"\n🔄 Processing endpoint: {endpoint}")
        try:
            records = await fetch_all_entities(endpoint)
            if not records or len(records) == 0:
                print(f"❌ No data found for {endpoint}. Skipping.")
                continue

            print(f"✅ Fetched {len(records)} records from DoorLoop for {endpoint}.")
            await upsert_raw_doorloop_data(endpoint, records, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
            print(f"✅ Successfully upserted {len(records)} records to Supabase raw tables for {endpoint}.")

        except Exception as e:
            print(f"❌ Failed to sync {endpoint}: {type(e).__name__}: {str(e)}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_master_sync())
