# 🛠 patched
from doorloop_sync.config import get_supabase_client, get_logger
from utils.decorators import task_error_handler

@task_error_handler
def run():
    supabase = get_supabase_client()
    logger = get_logger()
    logger.info("🧪 Normalization placeholder — implementation required.")
