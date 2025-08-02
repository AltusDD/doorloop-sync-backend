
# doorloop_sync/services/normalizers.py

def normalize_communications_record(raw_record):
    return {
        "id": raw_record.get("id"),
        "lease_id": raw_record.get("leaseId"),
        "type": raw_record.get("type"),
        "subject": raw_record.get("subject"),
        "body": raw_record.get("body"),
        "direction": raw_record.get("direction"),
        "sent_at": raw_record.get("sentAt"),
        "status": raw_record.get("status"),
        "created_at": raw_record.get("createdAt"),
        "updated_at": raw_record.get("updatedAt"),
    }

# Placeholder stubs for future normalization functions
def normalize_placeholder(*args, **kwargs):
    return {}
