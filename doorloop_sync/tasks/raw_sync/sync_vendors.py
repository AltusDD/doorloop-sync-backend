import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_vendors():
    logger.info("Starting raw sync for vendors...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/vendors")
    # Proceed with upsert or normalization logic here
