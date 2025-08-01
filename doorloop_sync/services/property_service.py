
import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import audit_log

class PropertySyncService:
    def __init__(self):
        self.doorloop = DoorLoopClient()
        self.supabase = SupabaseIngestClient()
        self.entity = "properties"

    def sync(self):
        logging.info(f"🔄 Syncing {self.entity} from DoorLoop...")
        try:
            records = self.doorloop.get_all("/properties")
            logging.info(f"📥 Retrieved {len(records)} properties from DoorLoop.")
            for record in records:
                self.supabase.store_raw(self.entity, record)
            audit_log("sync_success", f"{len(records)} records synced", entity=self.entity)
        except Exception as e:
            logging.exception(f"❌ Error syncing {self.entity}: {e}")
            audit_log("sync_error", str(e), entity=self.entity)
