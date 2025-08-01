import logging
import os
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import audit_log

class PropertySyncService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.doorloop = DoorLoopClient(
            base_url=os.environ["DOORLOOP_API_BASE_URL"],
            api_key=os.environ["DOORLOOP_API_KEY"]
        )
        self.supabase = SupabaseIngestClient()
        self.entity = "PropertySyncService"

    def sync(self):
        try:
            self.logger.info("🚀 Syncing properties from DoorLoop...")
            audit_log("sync_start", "Beginning property sync", entity=self.entity)

            properties = self.doorloop.get_all(endpoint="properties")
            self.logger.info(f"📥 Retrieved {len(properties)} properties")
            audit_log("sync_data_received", f"Retrieved {len(properties)} properties", entity=self.entity)

            normalized = self.normalize_properties(properties)
            self.logger.info(f"🔁 Normalized {len(normalized)} properties")
            audit_log("sync_normalized", f"Normalized {len(normalized)} properties", entity=self.entity)

            self.supabase.upsert(table="doorloop_raw_properties", data=normalized)
            self.logger.info(f"📤 Upserted {len(normalized)} records into Supabase")
            audit_log("sync_complete", f"Upserted {len(normalized)} records", entity=self.entity)

        except Exception as e:
            self.logger.error(f"❌ Error syncing {self.entity}", exc_info=True)
            audit_log("sync_error", str(e), entity=self.entity)

    def normalize_properties(self, records: list[dict]) -> list[dict]:
        normalized = []
        for record in records:
            normalized.append({
                "doorloop_id": record.get("id"),
                "name": record.get("name"),
                "address_street1": record.get("address", {}).get("street1"),
                "address_city": record.get("address", {}).get("city"),
                "address_state": record.get("address", {}).get("state"),
                "zip": record.get("address", {}).get("zip"),
                "type": record.get("type"),
                "created_at": record.get("createdAt"),
                "updated_at": record.get("updatedAt"),
                "raw_json": record,
            })
        return normalized
