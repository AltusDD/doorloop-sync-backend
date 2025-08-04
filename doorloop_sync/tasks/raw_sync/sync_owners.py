import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_owners():
    logger.info("Starting raw sync for owners...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/owners")
    # Proceed with upsert or normalization logic here
