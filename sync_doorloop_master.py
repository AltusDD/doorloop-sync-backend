from doorloop_client import fetch_all_doorloop_data
from supabase_client import insert_raw_data

def main():
    print("🚀 Starting master DoorLoop sync...")

    endpoint = "leases"  # You can loop through all endpoints later
    all_data = fetch_all_doorloop_data(endpoint)

    print(f"✅ Synced {len(all_data)} records from /{endpoint}")

    if all_data:
        insert_raw_data(endpoint, all_data)
        print(f"📦 Inserted into Supabase: /{endpoint}")
    else:
        print("⚠️ No data to insert.")

if __name__ == "__main__":
    main()
