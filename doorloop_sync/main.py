
from doorloop_sync.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, DOORLOOP_API_KEY, DOORLOOP_API_BASE_URL
from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.clients.doorloop_client import DoorLoopClient

from doorloop_sync.tasks.sync_raw import sync_properties, sync_units, sync_leases, sync_tenants, sync_owners
from doorloop_sync.tasks.normalize import normalize_properties, normalize_units, normalize_leases, normalize_tenants, normalize_owners

def run_task(task_fn, name, client):
    try:
        print(f"🚀 Starting task: {name}")
        task_fn.run(client)
        print(f"✅ Completed task: {name}")
    except Exception as e:
        print(f"❌ Task failed: {name} with error: {str(e)}")

if __name__ == "__main__":
    doorloop_client = DoorLoopClient(DOORLOOP_API_BASE_URL, DOORLOOP_API_KEY)
    supabase_client = SupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    RAW_SYNC_TASKS = [
        ("sync_properties", sync_properties),
        ("sync_units", sync_units),
        ("sync_leases", sync_leases),
        ("sync_tenants", sync_tenants),
        ("sync_owners", sync_owners),
    ]

    NORMALIZATION_TASKS = [
        ("normalize_properties", normalize_properties),
        ("normalize_units", normalize_units),
        ("normalize_leases", normalize_leases),
        ("normalize_tenants", normalize_tenants),
        ("normalize_owners", normalize_owners),
    ]

    print("🔄 Starting RAW SYNC tasks...")
    for name, task in RAW_SYNC_TASKS:
        run_task(task, name, doorloop_client)

    print("🧠 Starting NORMALIZATION tasks...")
    for name, task in NORMALIZATION_TASKS:
        run_task(task, name, supabase_client)
