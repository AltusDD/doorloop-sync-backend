# doorloop_sync/clients/doorloop_client.py

import os
import requests

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def get(self, endpoint: str):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
