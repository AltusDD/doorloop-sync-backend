import os
import requests
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL").rstrip("/") + "/"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get_all(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        logger.info(f"📡 GET {url}")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        elif isinstance(data, list):
            return data
        return []
