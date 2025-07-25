from supabase import get_client
from util.helpers import upsert_record
from util.logger import log_pipeline_event

def normalize_tasks():
    db = get_client()
    raw_records = db.table("doorloop_raw_tasks").select("*").execute().data
    for record in raw_records:
        try:
            norm = {
                "doorloop_id": record["id"],
                "title": record.get("title"),
                "due_date": record.get("dueDate"),
                "assigned_to": record.get("assignedTo"),
                "entity_type": record.get("entityType"),
                "entity_id": record.get("entityId")
            }
            upsert_record("tasks", norm)
            log_pipeline_event("tasks", record["id"], status="success")
        except Exception as e:
            log_pipeline_event("tasks", record.get("id"), status="failed", error=str(e))
