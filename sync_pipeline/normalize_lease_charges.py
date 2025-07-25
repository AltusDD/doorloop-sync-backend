from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_lease_charges():
    db = get_client()
    raw_records = db.table("doorloop_raw_lease_charges").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "lease_id": db.map_doorloop_id_to_internal("leases", record["leaseId"]),
                "amount": record.get("amountDue"),
                "due_date": record.get("dueDate"),
                "description": record.get("description"),
                "charge_type": record.get("type")
            }
            upsert_record("lease_charges", norm)
            log_pipeline_event("lease_charges", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("lease_charges", record.get("id"), status="failed", error=str(e))
