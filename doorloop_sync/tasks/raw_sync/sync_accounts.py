from doorloop_sync.clients.doorloop_client import DoorLoopClient
import os
import logging

logger = logging.getLogger(__name__)

def sync_accounts():
    logger.info("Starting raw sync for accounts...")

    api_key = os.environ.get("DOORLOOP_API_KEY")
    base_url = os.environ.get("DOORLOOP_API_BASE_URL")

    if not api_key or not base_url:
        raise ValueError("Missing DOORLOOP_API_KEY or DOORLOOP_API_BASE_URL environment variables.")

    doorloop = DoorLoopClient(api_key=api_key, base_url=base_url)
    all_records = doorloop.get_all("/api/accounts")

    logger.info(f"✅ Synced {len(all_records)} accounts.")
