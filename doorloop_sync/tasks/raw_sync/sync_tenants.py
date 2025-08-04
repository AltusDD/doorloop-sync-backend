import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_tenants():
    logger.info("Starting raw sync for tenants...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/tenants")
    # Proceed with upsert or normalization logic here
