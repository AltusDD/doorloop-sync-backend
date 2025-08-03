from doorloop_sync.clients import DoorLoopClient
from doorloop_sync.config import get_supabase_client
from doorloop_sync.utils.transform import flatten_dict
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler
def run():
    print("🔄 Syncing Recurring charges...")
    doorloop = DoorLoopClient()
    supabase = get_supabase_client()

    try:
        all_data = doorloop.get_entities("recurring-charges")
        flattened_data = [flatten_dict(record) for record in all_data]
        supabase.insert_bulk("doorloop_raw_recurring_charges", flattened_data)
    except Exception as e:
        print(f"❌ Error syncing Recurring charges: {e}")
        raise
