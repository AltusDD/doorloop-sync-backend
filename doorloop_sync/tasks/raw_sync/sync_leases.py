import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_leases():
    logger.info("Starting raw sync for leases...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/leases")
    # Proceed with upsert or normalization logic here
