"""
Task: Sync_Raw Accounts
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/accounts")
    print("✅ Sync_Raw accounts fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
