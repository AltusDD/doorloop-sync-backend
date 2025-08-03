from doorloop_sync.config import get_doorloop_client, get_supabase_client
from doorloop_sync.utils.transform import flatten_dict
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler
def run():
    doorloop = get_doorloop_client()
    supabase = get_supabase_client()

    response = doorloop.get("/recurring-charges", params={"page": 1})
    records = response.get("data", [])

    normalized_records = [flatten_dict(record) for record in records]
    supabase.table("doorloop_raw_recurring_charges").upsert(normalized_records).execute()
