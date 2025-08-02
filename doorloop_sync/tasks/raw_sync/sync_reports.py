"""
Task: Sync_Raw Reports
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/reports")
    print("✅ Sync_Raw reports fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
