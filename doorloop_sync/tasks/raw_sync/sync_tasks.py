import logging
from doorloop_sync.clients.doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_tasks():
    logger.info("Starting raw sync for tasks...")
    doorloop = DoorLoopClient()
    all_records = doorloop.get_all("/api/tasks")
    # Proceed with upsert or normalization logic here
