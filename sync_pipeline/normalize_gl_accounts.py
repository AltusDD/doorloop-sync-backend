from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_gl_accounts():
    db = get_client()
    raw_records = db.table("doorloop_raw_gl_accounts").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "name": record.get("name"),
                "account_type": record.get("accountType")
            }
            upsert_record("gl_accounts", norm)
            log_pipeline_event("gl_accounts", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("gl_accounts", record.get("id"), status="failed", error=str(e))
