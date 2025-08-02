"""
Task: Sync_Raw Vendors
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/vendors")
    print("✅ Sync_Raw vendors fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
