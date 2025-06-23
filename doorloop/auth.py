import os

def get_auth_headers():
    return {
        "Authorization": f"Bearer {os.getenv('DOORLOOP_API_KEY')}",
        "Content-Type": "application/json"
    }
