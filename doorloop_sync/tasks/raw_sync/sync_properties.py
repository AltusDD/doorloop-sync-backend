
import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_properties():
    logger.info("🚀 Starting sync for: properties")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/properties")
    if not all_records:
        logger.warning("⚠️ No properties data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)  # Ensure active column exists

    response = supabase.upsert("properties", all_records)
    logger.info(f"✅ Completed sync for properties. Records processed: {len(all_records)}")
    return response
