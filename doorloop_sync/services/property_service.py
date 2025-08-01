
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.config import DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

class PropertySyncService:
    def __init__(self):
        self.doorloop = DoorLoopClient(
            base_url=DOORLOOP_API_BASE_URL,
            api_key=DOORLOOP_API_KEY
        )
        self.supabase = SupabaseIngestClient(
            supabase_url=SUPABASE_URL,
            supabase_service_key=SUPABASE_SERVICE_ROLE_KEY
        )

    def sync(self):
        print("✅ Syncing PropertySyncService... (stub logic)")
        # TODO: Replace with real fetch + upsert logic
