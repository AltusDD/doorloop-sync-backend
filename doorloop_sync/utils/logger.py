from datetime import datetime

def _ts():
    return datetime.utcnow().isoformat()

def log_sync_start(entity):
    print(f"[{_ts()}] 🚀 Starting sync for: {entity}")

def log_sync_end(entity, count):
    print(f"[{_ts()}] ✅ Completed sync for {entity}. Records processed: {count}")

def log_error(entity, error):
    print(f"[{_ts()}] ❌ ERROR during sync for {entity}: {error}")

def log_insert_success(entity, count):
    print(f"[{_ts()}] 📥 Inserted {count} records into {entity} table.")

def log_insert_failure(entity, code, message):
    print(f"[{_ts()}] 🛑 Failed to insert into {entity} table. Status: {code}, Message: {message}")
