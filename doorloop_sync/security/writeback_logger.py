
# writeback_logger.py

def log_write_attempt(user, method, endpoint, data):
    from datetime import datetime
    with open("writeback_audit.log", "a") as f:
        f.write(f"[{datetime.now()}] User: {user['name']} | Role: {user['role']} | Method: {method} | Endpoint: {endpoint} | Data: {data}\n")
