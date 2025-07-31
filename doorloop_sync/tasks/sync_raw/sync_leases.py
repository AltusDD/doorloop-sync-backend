
def run(doorloop_client):
    print("🔄 Running sync_leases task...")
    records = doorloop_client.fetch("leases")
    print(f"Fetched {len(records)} records from DoorLoop endpoint: leases")
