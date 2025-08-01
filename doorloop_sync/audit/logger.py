import logging

logger = logging.getLogger(__name__)

def audit_log(event_type: str, message: str, entity: str = None):
    formatted_message = f"[AUDIT] {event_type}: {message}"
    if entity:
        formatted_message += f" | Entity: {entity}"
    logger.info(formatted_message)
