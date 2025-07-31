def audit_log(entity_type: str, status: str, message: str):
    print(f"[AUDIT] {entity_type.upper()} | {status} | {message}")
