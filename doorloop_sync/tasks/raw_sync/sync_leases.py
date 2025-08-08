from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_leases():
    """
    Fetches all leases from DoorLoop and upserts them into the 'leases' table.
    Stores DoorLoop IDs for related entities, which will be linked in a later
    normalization step.
    """
    entity = "leases"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/leases")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            # Safely get the first unit and tenant from the lists
            primary_unit_id = (item.get("units") or [None])[0]
            primary_tenant_id = (item.get("tenants") or [None])[0]

            record = {
                "doorloop_id": item.get("id"),
                "name": item.get("name"),
                "start_date": item.get("start"),
                "end_date": item.get("end"),
                "term": item.get("term"),
                "status": item.get("status"),
                "eviction_pending": item.get("evictionPending"),
                "rollover_to_at_will": item.get("rolloverToAtWill"),
                "total_balance_due_cents": int(float(item.get("totalBalanceDue", 0)) * 100),
                "total_deposits_held_cents": int(float(item.get("totalDepositsHeld", 0)) * 100),
                "total_recurring_rent_cents": int(float(item.get("totalRecurringRent", 0)) * 100),
                "property_id_dl": item.get("property"),
                "unit_id_dl": primary_unit_id,
                "tenant_id_dl": primary_tenant_id,
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("leases", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_leases.py [silent tag]
