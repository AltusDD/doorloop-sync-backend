from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.config import get_doorloop_client, get_supabase_client
from doorloop_sync.utils.transform import flatten_dict
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler
def run():
    doorloop = get_doorloop_client()
    supabase = get_supabase_client()

    accounts = doorloop.get_all("accounts")
    if not accounts:
        print("No accounts found.")
        return

    records = [flatten_dict(a) for a in accounts]
    supabase.upsert("doorloop_raw_accounts", records)