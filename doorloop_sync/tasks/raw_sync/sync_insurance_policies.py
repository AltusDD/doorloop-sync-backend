from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_insurance_policies():
    """
    Fetches all insurance policies from DoorLoop and upserts them into the
    'insurance_policies' table.
    """
    entity = "insurance_policies"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/insurance-policies")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "provider": item.get("provider"),
                "policy_number": item.get("policyNumber"),
                "coverage_cents": int(float(item.get("coverage", 0)) * 100),
                "effective_date": item.get("effectiveDate"),
                "expiration_date": item.get("expirationDate"),
                "resource_id_dl": item.get("linkedResourceId"),
                "resource_type": item.get("linkedResourceType"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("insurance_policies", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_insurance_policies.py [silent tag]
