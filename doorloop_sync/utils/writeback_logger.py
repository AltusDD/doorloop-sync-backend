# doorloop_sync/utils/writeback_logger.py

def log_write_attempt(user_id: str, action: str, entity: str, entity_id: str, status: str, message: str = ""):
    """
    Logs all write attempts to DoorLoop, regardless of success/failure.

    Args:
        user_id (str): ID of the user attempting the write.
        action (str): Action performed (e.g., PATCH, POST, DELETE).
        entity (str): The type of entity affected (e.g., 'lease', 'property').
        entity_id (str): The unique ID of the entity.
        status (str): Result of the write ('success' or 'fail').
        message (str, optional): Any additional context or error message.
    """
    # Placeholder: Replace with actual database or logging system call
    print(f"[WRITEBACK LOG] user={user_id}, action={action}, entity={entity}, id={entity_id}, status={status}, message={message}")
