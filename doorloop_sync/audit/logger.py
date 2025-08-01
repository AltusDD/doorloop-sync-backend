from typing import Optional

def audit_log(event_type: str, message: str, entity: Optional[str] = None):
    print(f"[AUDIT] {event_type}: {message} | Entity: {entity}")
