import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_files():
    logger.info("🚀 Starting sync for: files")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/files")
    if not all_records:
        logger.warning(f"⚠️ No files data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)

    response = supabase.upsert("files", all_records)
    logger.info(f"✅ Completed sync for files. Records processed: {len(all_records)}")
    return response
