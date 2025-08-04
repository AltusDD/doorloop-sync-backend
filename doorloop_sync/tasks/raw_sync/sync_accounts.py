import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_accounts():
    logger.info("Starting raw sync for accounts...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/accounts")
    # Proceed with upsert or normalization logic here
