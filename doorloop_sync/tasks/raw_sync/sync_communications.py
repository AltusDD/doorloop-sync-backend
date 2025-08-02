"""
Task: Sync_Raw Communications
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/communications")
    print("✅ Sync_Raw communications fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
