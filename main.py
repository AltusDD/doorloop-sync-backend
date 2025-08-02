
from doorloop_sync.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.tasks.normalize import normalize_properties

def run_task(task_fn, name):
    try:
        print(f"🚀 Starting task: {name}")
        task_fn.run(SupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY))
        print(f"✅ Completed task: {name}")
    except Exception as e:
        print(f"❌ Task failed: {name} with error: {str(e)}")

if __name__ == "__main__":
    tasks = [
        ("normalize_properties", normalize_properties),
    ]

    for name, task in tasks:
        run_task(task, name)
