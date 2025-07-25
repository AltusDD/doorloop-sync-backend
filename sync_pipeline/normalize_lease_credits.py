from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_lease_credits():
    db = get_client()
    raw_records = db.table("doorloop_raw_lease_credits").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "lease_id": db.map_doorloop_id_to_internal("leases", record["leaseId"]),
                "amount": record.get("amount"),
                "credit_date": record.get("creditDate"),
                "description": record.get("description")
            }
            upsert_record("lease_credits", norm)
            log_pipeline_event("lease_credits", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("lease_credits", record.get("id"), status="failed", error=str(e))
