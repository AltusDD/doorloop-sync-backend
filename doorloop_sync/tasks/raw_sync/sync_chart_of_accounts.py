from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.utils.data_processing import standardize_records

def sync_chart_of_accounts():
    endpoint = "/api/chart-of-accounts"
    print(f"Starting raw sync for {endpoint}...")

    doorloop = DoorLoopClient()
    supabase = SupabaseClient()

    all_records = doorloop.get_all(endpoint)
    standardized = standardize_records(all_records)
    if not standardized:
        print(f"No valid records for {endpoint}, skipping...")
        return

    supabase.upsert_raw_records(entity="chart_of_accounts", records=standardized)
