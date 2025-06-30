import os
from doorloop_client import fetch_all_doorloop_data
from supabase_client import insert_raw_data

# The DoorLoop endpoint to fetch and sync
endpoint = "/leases"

def main():
    print("🚀 Starting master DoorLoop sync...")

    # Fetch all data from DoorLoop
    all_data = fetch_all_doorloop_data(endpoint)
    print(f"✅ Synced {len(all_data)} records from {endpoint}")

    # Insert raw data into Supabase
    insert_raw_data(all_data, endpoint)
    print(f"📥 Inserted into Supabase: {endpoint}")

if __name__ == "__main__":
    main()
