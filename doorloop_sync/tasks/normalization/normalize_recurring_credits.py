from doorloop_sync.clients import DoorLoopClient
from doorloop_sync.config import get_supabase_client
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler  # ✅ Correct usage
def run():
    print("🔄 Syncing Recurring Credits...")
    doorloop = DoorLoopClient()
    supabase = get_supabase_client()
    data = doorloop.get_all("recurring-credits")
    supabase.upsert("doorloop_raw_recurring_credits", data)
