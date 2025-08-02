import logging
from doorloop_sync.config import supabase_client
from doorloop_sync.services.audit_logger import log_audit_event

logger = logging.getLogger(__name__)

def normalize_activity_logs():
    entity = "activity_logs"
    logger.info(f"🔄 Starting normalization for {entity}")

    try:
        raw_data_response = supabase_client.from_("doorloop_raw_activity_logs").select("*").execute()
        raw_records = raw_data_response.data or []

        if not raw_records:
            logger.info(f"📭 No raw data found for {entity}")
            return

        normalized_records = []
        for record in raw_records:
            raw = record.get("raw", {})
            normalized = {
                "doorloop_id": raw.get("id"),
                "entity_type": raw.get("entityType"),
                "entity_id": raw.get("entityId"),
                "action": raw.get("action"),
                "user_id": raw.get("userId"),
                "timestamp": raw.get("timestamp"),
                "created_at": record.get("created_at"),
            }
            normalized_records.append(normalized)

        supabase_client.upsert("doorloop_normalized_activity_logs", normalized_records, on_conflict=["doorloop_id"])

        log_audit_event(
            entity=entity,
            action="normalize",
            status="success",
            message=f"✅ Normalized {len(normalized_records)} records"
        )
    except Exception as e:
        logger.error(f"❌ Failed to normalize {entity}: {str(e)}")
        log_audit_event(
            entity=entity,
            action="normalize",
            status="error",
            message=str(e)
        )
