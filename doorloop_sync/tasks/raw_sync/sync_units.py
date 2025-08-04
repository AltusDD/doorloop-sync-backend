import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_units():
    logger.info("Starting raw sync for units...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/units")
    # Proceed with upsert or normalization logic here
