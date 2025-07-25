# normalize_tasks.py - Empire Grade Phase 2
from supabase import create_client
from utils import normalize_record, insert_normalized_data

def normalize_tasks(raw_tasks):
    normalized = []
    for task in raw_tasks:
        normalized.append({
            "id": task.get("id"),
            "doorloop_id": task.get("id"),
            "title": task.get("title", "").strip(),
            "description": task.get("description", "").strip(),
            "status": task.get("status"),
            "assigned_to_id": task.get("assignedToId"),
            "property_id": task.get("propertyId"),
            "unit_id": task.get("unitId"),
            "due_date": task.get("dueDate"),
            "created_at": task.get("createdAt"),
            "updated_at": task.get("updatedAt"),
        })
    return normalized

def run():
    client = create_client()
    raw = client.table("doorloop_raw_tasks").select("*").execute()
    tasks = normalize_tasks(raw.data)
    insert_normalized_data("doorloop_normalized_tasks", tasks)

if __name__ == "__main__":
    run()
