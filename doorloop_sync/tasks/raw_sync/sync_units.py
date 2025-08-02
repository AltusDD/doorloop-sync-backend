
def run(doorloop_client):
    print("🔄 Running sync_units task...")
    records = doorloop_client.fetch("units")
    print(f"Fetched {len(records)} records from DoorLoop endpoint: units")
