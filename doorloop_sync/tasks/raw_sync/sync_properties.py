
def run(doorloop_client):
    print("🔄 Running sync_properties task...")
    records = doorloop_client.fetch("properties")
    print(f"Fetched {len(records)} records from DoorLoop endpoint: properties")
