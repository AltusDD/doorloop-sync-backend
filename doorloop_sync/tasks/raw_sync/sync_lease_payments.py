from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_lease_payments():
    """
    Fetches all lease payments from DoorLoop and upserts them into the
    'lease_payments' table.
    """
    entity = "lease_payments"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/lease-payments")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "amount_cents": int(float(item.get("amountReceived", 0)) * 100),
                "payment_date": item.get("date"),
                "status": item.get("depositStatus"),
                "payment_method": item.get("paymentMethod"),
                "lease_id_dl": item.get("lease"),
                "tenant_id_dl": item.get("receivedFromTenant"),
                "account_id_dl": item.get("depositToAccount"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("lease_payments", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_lease_payments.py [silent tag]
