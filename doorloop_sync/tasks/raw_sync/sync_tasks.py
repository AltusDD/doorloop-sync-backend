"""
Task: Sync_Raw Tasks
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/tasks")
    print("✅ Sync_Raw tasks fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
