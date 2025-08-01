from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

class PropertySyncService:
    def __init__(self):
        self.dl = DoorLoopClient()
        self.supabase = SupabaseIngestClient()

    def sync(self):
        print("🔁 Syncing properties...")
        raw_properties = self.dl.get("/properties")
        self.supabase.upsert("doorloop_raw_properties", raw_properties)