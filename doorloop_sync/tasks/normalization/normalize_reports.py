
from doorloop_sync.config import get_supabase_client
from utils.decorators import task_error_handler

supabase = get_supabase_client()

@task_error_handler
def normalize_reports():
    print("normalize_reports() running [silent update]")
