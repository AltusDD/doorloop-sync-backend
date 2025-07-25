
import time
import traceback
from audit_logger import log_pipeline_event  # This helper must exist
from supabase import insert_normalized_lease  # Assumed placeholder

def normalize_leases(leases_raw, batch_id):
    for lease in leases_raw:
        doorloop_id = lease.get("id")
        internal_id = None
        start_time = time.time()
        try:
            # Normalize and insert logic here
            internal_id = insert_normalized_lease(lease)  # Placeholder
            duration = int((time.time() - start_time) * 1000)
            log_pipeline_event(
                entity_type="lease",
                stage="normalize_leases",
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
                entity_type="lease",
                stage="normalize_leases",
                doorloop_id=doorloop_id,
                internal_id=internal_id,
                status="error",
                batch_id=batch_id,
                record_count=0,
                duration_ms=duration,
                error_details={
                    "error": str(e),
                    "trace": traceback.format_exc()
                }
            )
