from doorloop_client import fetch_all_doorloop_data

def main():
    print("🚀 Starting master DoorLoop sync...")

    # Example: sync 1 endpoint just to test (expand later)
    endpoint = "leases"
    all_data = fetch_all_doorloop_data(endpoint)

    print(f"✅ Synced {len(all_data)} records from /{endpoint}")
    # You can add a call to your Supabase insert function here later

if __name__ == "__main__":
    main()
