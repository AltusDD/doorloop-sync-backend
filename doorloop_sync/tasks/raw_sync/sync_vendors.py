import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_vendors():
    logger.info("🚀 Starting sync for: vendors")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/vendors")
    if not all_records:
        logger.warning(f"⚠️ No vendors data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)

    response = supabase.upsert("vendors", all_records)
    logger.info(f"✅ Completed sync for vendors. Records processed: {len(all_records)}")
    return response
