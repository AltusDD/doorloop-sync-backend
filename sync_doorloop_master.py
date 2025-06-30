import os
from doorloop_client import fetch_all_doorloop_data
from supabase_client import insert_raw_data

def main():
    print("🚀 Starting master DoorLoop sync...")

    # Specify the endpoint you want to sync
    endpoint = "/leases"

    # Fetch all data from DoorLoop
    all_data = fetch_all_doorloop_data(endpoint)

    # Ensure data is in a list format
    if isinstance(all_data, dict):
        all_data = [all_data]
    elif not isinstance(all_data, list):
        print(f"❌ Unexpected data type from API: {type(all_data)}")
        return

    print(f"✅ Synced {len(all_data)} records from {endpoint}")

    # Insert into Supabase
    insert_raw_data(all_data, endpoint)

if __name__ == "__main__":
    main()
