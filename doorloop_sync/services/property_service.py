
import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import audit_log
from doorloop_sync.normalization import normalize_properties

logger = logging.getLogger(__name__)

class PropertySyncService:
    def __init__(self):
        self.entity = "PropertySyncService"
        self.doorloop = DoorLoopClient()
        self.supabase = SupabaseIngestClient()

    def sync(self):
        try:
            logger.info("🚀 Syncing properties from DoorLoop...")
            audit_log("sync_start", "Beginning property sync", self.entity)

            logger.info("📡 Fetching all records from properties...")
            properties = self.doorloop.get_all("properties")
            logger.info(f"📥 Retrieved {len(properties)} properties")
            audit_log("sync_data_received", f"Retrieved {len(properties)} properties", self.entity)

            normalized = normalize_properties(properties)
            logger.info(f"🔁 Normalized {len(normalized)} properties")
            audit_log("sync_normalized", f"Normalized {len(normalized)} properties", self.entity)

            self.supabase.upsert("doorloop_raw_properties", normalized)

            logger.info("✅ Property sync complete.")
            audit_log("sync_complete", f"Synced {len(normalized)} properties", self.entity)

        except Exception as e:
            logger.error("❌ Error syncing PropertySyncService", exc_info=True)
            audit_log("sync_error", str(e), self.entity)
