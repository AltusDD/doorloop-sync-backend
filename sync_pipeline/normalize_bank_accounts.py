from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_bank_accounts():
    db = get_client()
    raw_records = db.table("doorloop_raw_bank_accounts").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "name": record.get("name"),
                "bank_name": record.get("bankName"),
                "account_number": record.get("accountNumber"),
                "routing_number": record.get("routingNumber")
            }
            upsert_record("bank_accounts", norm)
            log_pipeline_event("bank_accounts", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("bank_accounts", record.get("id"), status="failed", error=str(e))
