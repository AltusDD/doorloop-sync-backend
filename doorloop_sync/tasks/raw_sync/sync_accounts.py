
from doorloop_sync.clients.doorloop_client import DoorLoopClient
import os

def sync_accounts():
    print("Starting raw sync for accounts...")
    doorloop = DoorLoopClient(
        api_key=os.environ["DOORLOOP_API_KEY"],
        base_url=os.environ["DOORLOOP_API_BASE_URL"]
    )
    all_records = doorloop.get_all("/api/accounts", delay_between_pages=0.5)
    print(f"🔁 Synced {len(all_records)} accounts")
    # silent_tag: empire_sync_patch_v3_retry_delay
