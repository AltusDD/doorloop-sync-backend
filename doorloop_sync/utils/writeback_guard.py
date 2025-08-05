
# PATCH_SILENT_TAG: writeback_guard_update
ALLOWED_USERS = {
    "accounting@example.com": ["accounts", "tenants", "leases"],
    "maintenance@example.com": ["work_orders"],
    "legal@example.com": ["legal_cases"],
    "admin@example.com": ["*"]
}

def can_writeback(user_email, entity_type):
    permissions = ALLOWED_USERS.get(user_email, [])
    return "*" in permissions or entity_type in permissions

def enforce_writeback_permission(user_email, entity_type):
    if not can_writeback(user_email, entity_type):
        raise PermissionError(f"User {user_email} not authorized to modify {entity_type}.")
