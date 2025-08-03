# silent-tag: normalize-accounts-fix-0803
from doorloop_sync.config import get_supabase_client
from doorloop_sync.utils.decorators import task_error_handler

@task_error_handler
def run():
    print("Running normalize_accounts")
