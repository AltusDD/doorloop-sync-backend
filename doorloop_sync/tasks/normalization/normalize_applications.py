
from doorloop_sync.config import supabase_client
from doorloop_sync.services.audit_logger import log_audit_event

def run():
    try:
        raw_records = supabase_client.get_all("doorloop_raw_applications")
        normalized_records = []

        for record in raw_records:
            normalized_records.append({{
                # TODO: Map fields from raw record to normalized record
            }})

        supabase_client.upsert_many("doorloop_normalized_applications", normalized_records)
        log_audit_event(entity="normalize_applications", status="success", metadata={{"normalized_count": len(normalized_records)}})

    except Exception as e:
        log_audit_event(entity="normalize_applications", status="error", error=True, metadata={{"message": str(e)}})
        print(f"❌ Error in normalize_applications: {{str(e)}}")
