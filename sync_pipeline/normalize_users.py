from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_users():
    db = get_client()
    raw_records = db.table("doorloop_raw_users").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "first_name": record.get("firstName"),
                "last_name": record.get("lastName"),
                "email": record.get("email"),
                "role": record.get("role")
            }
            upsert_record("users", norm)
            log_pipeline_event("users", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("users", record.get("id"), status="failed", error=str(e))
