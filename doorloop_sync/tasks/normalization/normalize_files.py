from doorloop_sync.clients.supabase_client import SupabaseClient
from doorloop_sync.utils.logging import logger

def run():
    logger.info("🔁 Starting normalization for files...")

    supabase_client = SupabaseClient()
    raw_table_name = "doorloop_raw_files"
    normalized_table_name = "doorloop_normalized_files"

    raw_records = supabase_client.fetch_all(table=raw_table_name)

    if not raw_records:
        logger.info("📭 No raw files data to normalize. Task complete.")
        return

    normalized_records = []
    for record in raw_records:
        try:
            normalized = {
                "doorloop_id": record.get("id"),
                "name": record.get("name"),
                "entity_id": record.get("entityId"),
                "entity_type": record.get("entityType"),
                "mime_type": record.get("mimeType"),
                "file_size": record.get("size"),
                "created_at": record.get("createdAt"),
                "updated_at": record.get("updatedAt"),
            }
            normalized_records.append(normalized)
        except Exception as e:
            logger.warning(f"⚠️ Skipping malformed record: {e}")

    try:
        supabase_client.upsert(table=normalized_table_name, data=normalized_records)
        logger.info(f"✅ Normalized {len(normalized_records)} files into {normalized_table_name}")
    except Exception as e:
        logger.error(f"❌ Failed to upsert normalized files: {e}")
