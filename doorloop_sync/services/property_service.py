import logging
import os
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient

logger = logging.getLogger(__name__)

class PropertySyncService:
    def __init__(self):
        self.doorloop = DoorLoopClient(
            base_url=os.getenv("DOORLOOP_API_BASE_URL"),
            api_key=os.getenv("DOORLOOP_API_KEY"),
        )
        self.supabase = SupabaseIngestClient()

    def sync(self):
        logger.info("🚀 Syncing properties from DoorLoop...")
        properties = self.doorloop.get_all("properties")
        logger.info(f"📦 Retrieved {len(properties)} properties.")
        if not properties:
            logger.warning("⚠️ No properties returned from DoorLoop.")
            return
        self.supabase.upsert("doorloop_raw_properties", properties)
        logger.info("✅ Properties sync complete.")
