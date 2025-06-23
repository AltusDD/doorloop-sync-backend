"""
sync_api.py – DoorLoop API fetch layer
"""

import requests
from .auth import get_auth_headers

DOORLOOP_BASE_URL = "https://api.doorloop.com/v1"

def fetch_all(endpoint: str):
    url = f"{DOORLOOP_BASE_URL}/{endpoint}"
    headers = get_auth_headers()
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
