import logging
from doorloop_sync.tasks.raw_sync.sync_accounts import sync_accounts

logger = logging.getLogger(__name__)

def sync_all():
    logger.info("🔁 Starting full sync process...")
    sync_accounts()
