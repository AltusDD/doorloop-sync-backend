
def audit_log(event_type, message, entity_type=None):
    print(f"[AUDIT] {event_type}: {message} | Entity: {entity_type}")
