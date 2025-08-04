import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_users():
    logger.info("Starting raw sync for users...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/users")
    # Proceed with upsert or normalization logic here
