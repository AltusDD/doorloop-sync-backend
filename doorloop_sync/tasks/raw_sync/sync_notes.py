import logging
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_client import DoorLoopClient

logger = logging.getLogger(__name__)

def sync_notes():
    logger.info("🚀 Starting sync for: notes")
    client = DoorLoopClient()
    supabase = SupabaseIngestClient()

    all_records = client.fetch_all("/api/notes")
    if not all_records:
        logger.warning(f"⚠️ No notes data fetched.")
        return

    for record in all_records:
        record.setdefault("active", True)

    response = supabase.upsert("notes", all_records)
    logger.info(f"✅ Completed sync for notes. Records processed: {len(all_records)}")
    return response
