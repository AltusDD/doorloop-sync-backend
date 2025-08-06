
import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_owners():
    logger.info("🚀 Starting sync for: owners")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/owners")
    if not all_records:
        logger.warning("⚠️ No owners data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)

    response = supabase.upsert("owners", all_records)
    logger.info(f"✅ Completed sync for owners. Records processed: {len(all_records)}")
    return response
