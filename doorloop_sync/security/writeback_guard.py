
# writeback_guard.py

AUTHORIZED_USERS = {
    "accounting": ["PATCH", "POST"],
    "legal": ["POST"],
    "admin": ["PATCH", "POST", "DELETE"],
    "dion": ["PATCH", "POST", "DELETE"]
}

def check_permission(user, method, endpoint):
    user_role = user.get("role")
    if not user_role:
        return False
    allowed_methods = AUTHORIZED_USERS.get(user_role, [])
    return method.upper() in allowed_methods
