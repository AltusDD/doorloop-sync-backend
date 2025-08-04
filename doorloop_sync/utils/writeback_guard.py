# 🛠️ Empire Patch: Silent Tag Added

ROLE_PERMISSIONS = {
    'owner': {'GET', 'POST', 'PATCH', 'DELETE'},
    'accounting': {'GET', 'POST', 'PATCH'},
    'legal': {'GET', 'PATCH'},
    'maintenance': {'GET', 'PATCH'},
    'leasing': {'GET', 'POST'},
    'viewer': {'GET'}
}

ENTITY_PERMISSIONS = {
    'accounts': {'accounting', 'owner'},
    'tenants': {'leasing', 'owner'},
    'properties': {'leasing', 'maintenance', 'owner'},
    'leases': {'leasing', 'owner'},
    'work_orders': {'maintenance', 'owner'},
    'legal_cases': {'owner'},
}


def is_write_allowed(user_role, method, entity_type):
    if method not in {'POST', 'PATCH', 'DELETE'}:
        return True
    allowed_roles = ENTITY_PERMISSIONS.get(entity_type, set())
    return user_role in allowed_roles and method in ROLE_PERMISSIONS.get(user_role, set())