from doorloop_client import fetch_all_doorloop_data
from supabase_client import insert_raw_data

endpoint = "/leases"

def main():
    print("🚀 Starting master DoorLoop sync...")
    all_data = fetch_all_doorloop_data(endpoint)

    # Add endpoint label to each item
    for item in all_data:
        item["endpoint"] = endpoint

    print(f"✅ Synced {len(all_data)} records from {endpoint}")
    insert_raw_data(all_data)

if __name__ == "__main__":
    main()
