
def run(doorloop_client):
    print("🔄 Running sync_tenants task...")
    records = doorloop_client.fetch("tenants")
    print(f"Fetched {len(records)} records from DoorLoop endpoint: tenants")
