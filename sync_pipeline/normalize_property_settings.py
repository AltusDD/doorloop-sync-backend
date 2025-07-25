from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_property_settings():
    db = get_client()
    raw_records = db.table("doorloop_raw_property_settings").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "property_id": db.map_doorloop_id_to_internal("properties", record.get("propertyId")),
                "setting_key": record.get("key"),
                "setting_value": record.get("value")
            }
            upsert_record("property_settings", norm)
            log_pipeline_event("property_settings", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("property_settings", record.get("id"), status="failed", error=str(e))
