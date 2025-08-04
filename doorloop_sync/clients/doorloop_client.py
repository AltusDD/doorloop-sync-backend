import os
import requests
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key or os.getenv("DOORLOOP_API_KEY")
        self.base_url = base_url or os.getenv("DOORLOOP_API_BASE_URL")
        if not self.api_key or not self.base_url:
            raise ValueError("Missing DoorLoop API key or base URL.")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        logger.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}")

    def get_all(self, entity):
        url = f"{self.base_url}/{entity}"
        logger.info(f"📡 GET {url}")
        response = requests.get(url, headers=self.headers)
        try:
            return response.json()
        except Exception:
            logger.error(f"❌ Failed to decode JSON from {url}: {response.text}")
            raise
