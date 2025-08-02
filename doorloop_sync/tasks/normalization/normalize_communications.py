import logging
from doorloop_sync.config import supabase_client
from doorloop_sync.services.audit_logger import log_audit_event
from doorloop_sync.services.normalizers import normalize_communications_record

logger = logging.getLogger(__name__)

def normalize_communications():
    logger.info("🔁 Starting normalization: communications")
    log_audit_event("normalize_communications", "started")

    raw_data = supabase_client.fetch_table("doorloop_raw_communications")
    logger.info(f"📥 Fetched {len(raw_data)} raw communications records")

    normalized_data = []
    for record in raw_data:
        try:
            normalized = normalize_communications_record(record)
            if normalized:
                normalized_data.append(normalized)
        except Exception as e:
            logger.warning(f"⚠️ Skipping record due to error: {e}")

    logger.info(f"📦 Normalized {len(normalized_data)} communications records")
    supabase_client.upsert_rows("doorloop_normalized_communications", normalized_data)

    log_audit_event("normalize_communications", "completed", count=len(normalized_data))
    logger.info("✅ Normalization completed: communications")
