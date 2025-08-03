from doorloop_sync.config import get_supabase_client, get_doorloop_client
from doorloop_sync.utils.transform import flatten_dict
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler
def run():
    supabase = get_supabase_client()
    doorloop = get_doorloop_client()

    data = doorloop.get("/portfolios")
    flattened_data = [flatten_dict(record) for record in data]
    supabase.table("doorloop_raw_portfolios").upsert(flattened_data).execute()
