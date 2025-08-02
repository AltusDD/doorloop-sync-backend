"""
Task: Sync_Raw Activity Logs
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/activity-logs")
    print("✅ Sync_Raw activity_logs fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
