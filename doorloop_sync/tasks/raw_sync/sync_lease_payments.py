import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_lease_payments():
    logger.info("Starting raw sync for lease-payments...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/lease-payments")
    # Proceed with upsert or normalization logic here
