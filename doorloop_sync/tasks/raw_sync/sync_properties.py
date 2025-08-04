import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_properties():
    logger.info("Starting raw sync for properties...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/properties")
    # Proceed with upsert or normalization logic here
