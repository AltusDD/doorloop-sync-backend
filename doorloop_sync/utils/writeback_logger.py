# 🛠️ Empire Patch: Silent Tag Added

import logging
import os
from datetime import datetime

log_file_path = os.path.join(os.path.dirname(__file__), 'writeback_audit.log')

def log_write_attempt(user_role, method, endpoint, entity_type):
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.utcnow().isoformat()} | {user_role.upper()} | {method.upper()} | {endpoint} | ENTITY: {entity_type}
")