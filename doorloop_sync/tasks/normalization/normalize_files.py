# doorloop_sync/tasks/normalization/normalize_files.py

from doorloop_sync.config import supabase_client
from doorloop_sync.services.audit_logger import log_audit_event

def normalize_files():
    entity = "files"
    try:
        raw_data = supabase_client.fetch_raw_table(f"doorloop_raw_{entity}")
        normalized_records = []

        for record in raw_data:
            data = record.get("raw_json", {})
            normalized = {
                "doorloop_id": data.get("id"),
                "name": data.get("name"),
                "description": data.get("description"),
                "type": data.get("type"),
                "size": data.get("size"),
                "created_at": data.get("createdAt"),
                "updated_at": data.get("updatedAt"),
                "related_entity_type": data.get("relatedToType"),
                "related_entity_id": data.get("relatedToId"),
            }
            normalized_records.append(normalized)

        supabase_client.upsert_normalized_table(
            table_name=f"doorloop_normalized_{entity}",
            records=normalized,
            match_columns=["doorloop_id"],
        )
        log_audit_event(entity=entity, status="success", message="Normalized files data.")

    except Exception as e:
        log_audit_event(entity=entity, status="error", message=str(e))
        raise e
