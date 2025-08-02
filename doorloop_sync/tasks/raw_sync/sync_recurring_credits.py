"""
Task: Sync_Raw Recurring Credits
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/recurring-credits")
    print("✅ Sync_Raw recurring_credits fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
