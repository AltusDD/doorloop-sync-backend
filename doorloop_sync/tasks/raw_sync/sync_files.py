"""
Task: Sync_Raw Files
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/files")
    print("✅ Sync_Raw files fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
