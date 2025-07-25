from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_vendors():
    db = get_client()
    raw_records = db.table("doorloop_raw_vendors").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "name": record.get("name"),
                "phone": record.get("phone"),
                "email": record.get("email"),
                "category": record.get("category")
            }
            upsert_record("vendors", norm)
            log_pipeline_event("vendors", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("vendors", record.get("id"), status="failed", error=str(e))
