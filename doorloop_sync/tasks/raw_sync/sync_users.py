"""
Task: Sync_Raw Users
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/users")
    print("✅ Sync_Raw users fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
