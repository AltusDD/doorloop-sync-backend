import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("audit")

def audit_log(status, message, entity=None, entity_type="sync"):
    log_record = {
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow().isoformat(),
        "entity": entity,
        "entity_type": entity_type,
    }
    logger.info(f"📝 Audit Log: {log_record}")
    return log_record