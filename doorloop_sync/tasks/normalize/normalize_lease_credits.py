"""
Task: Normalize Lease Credits
"""
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

def run():
    doorloop = DoorLoopClient()
    data = doorloop.fetch_all("/lease-credits")
    print("✅ Normalize lease_credits fetched", len(data))
    # TODO: Implement full ingest

if __name__ == "__main__":
    run()
