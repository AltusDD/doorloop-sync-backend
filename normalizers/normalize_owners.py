
import time
import traceback
from audit_logger import log_pipeline_event  # Ensure this exists
from supabase import insert_normalized_owner  # Placeholder function

def normalize_owners(raw_data, batch_id):
    for record in raw_data:
        doorloop_id = record.get("id")
        internal_id = None
        start_time = time.time()
        try:
            internal_id = insert_normalized_owner(record)  # Replace with actual insert logic
            duration = int((time.time() - start_time) * 1000)
            log_pipeline_event(
                entity_type="owner",
                stage="normalize_owners",
                doorloop_id=doorloop_id,
                internal_id=internal_id,
                status="success",
                batch_id=batch_id,
                record_count=1,
                duration_ms=duration
            )
        except Exception as e:
            duration = int((time.time() - start_time) * 1000)
            log_pipeline_event(
                entity_type="owner",
                stage="normalize_owners",
                doorloop_id=doorloop_id,
                internal_id=internal_id,
                status="error",
                batch_id=batch_id,
                record_count=0,
                duration_ms=duration,
                error_details={{
                    "error": str(e),
                    "trace": traceback.format_exc()
                }}
            )
