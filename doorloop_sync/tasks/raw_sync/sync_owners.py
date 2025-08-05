import os
from doorloop_sync.clients.doorloop_client import DoorLoopClient

def sync_owners():
    doorloop = DoorLoopClient(api_key=os.getenv("DOORLOOP_API_KEY"), base_url=os.getenv("DOORLOOP_API_BASE_URL"))
    endpoint = "/api/owners"
    all_records = doorloop.get_all(endpoint)
    # Placeholder for upsert or process logic
