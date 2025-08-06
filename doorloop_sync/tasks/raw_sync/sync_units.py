
import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_units():
    logger.info("🚀 Starting sync for: units")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/units")
    if not all_records:
        logger.warning("⚠️ No units data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)

    response = supabase.upsert("units", all_records)
    logger.info(f"✅ Completed sync for units. Records processed: {len(all_records)}")
    return response
