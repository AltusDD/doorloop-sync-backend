from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_work_orders():
    db = get_client()
    raw_records = db.table("doorloop_raw_work_orders").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "property_id": db.map_doorloop_id_to_internal("properties", record.get("propertyId")),
                "unit_id": db.map_doorloop_id_to_internal("units", record.get("unitId")),
                "vendor_id": db.map_doorloop_id_to_internal("vendors", record.get("vendorId")),
                "status": record.get("status"),
                "description": record.get("description"),
                "scheduled_date": record.get("scheduledDate"),
                "completed_date": record.get("completedDate")
            }
            upsert_record("work_orders", norm)
            log_pipeline_event("work_orders", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("work_orders", record.get("id"), status="failed", error=str(e))
