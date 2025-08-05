from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient

def sync_accounts():
    print("Starting raw sync for accounts...")
    doorloop = DoorLoopClient()
    supabase = SupabaseClient()
    all_records = doorloop.get_all("/api/accounts")
    valid_records = [r for r in all_records if isinstance(r, dict)]
    if not valid_records:
        print("⏭️ Skipping upsert to 'doorloop_raw_accounts' — no valid records after standardization.")
        return
    supabase.upsert("doorloop_raw_accounts", valid_records)
# [silent tag]