
def run(doorloop_client):
    print("🔄 Running sync_owners task...")
    records = doorloop_client.fetch("owners")
    print(f"Fetched {len(records)} records from DoorLoop endpoint: owners")
