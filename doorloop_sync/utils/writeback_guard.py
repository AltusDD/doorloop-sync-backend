# doorloop_sync/utils/writeback_guard.py

AUTHORIZED_ROLES = {
    "admin": ["POST", "PATCH", "DELETE"],
    "accounting": ["POST", "PATCH"],
    "legal": ["POST"],
    "maintenance": [],
    "leasing": [],
    "viewer": [],
}

def is_write_allowed(user_role: str, method: str) -> bool:
    """
    Determines if the user with given role can perform the specified write operation.

    Args:
        user_role (str): Role of the user (e.g., 'admin', 'accounting', etc.).
        method (str): HTTP method (e.g., 'POST', 'PATCH', 'DELETE').

    Returns:
        bool: True if allowed, False otherwise.
    """
    allowed_methods = AUTHORIZED_ROLES.get(user_role, [])
    return method.upper() in allowed_methods
