
import requests
import os
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, api_key=None, base_url=None):
        self.api_key = api_key or os.environ.get("DOORLOOP_API_KEY")
        self.base_url = (base_url or os.environ.get("DOORLOOP_API_BASE_URL")).rstrip("/") + "/"

        if not self.api_key or not self.base_url:
            raise ValueError("Missing DoorLoop API key or base URL.")

        logger.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def get_all(self, endpoint):
        url = f"{self.base_url}{endpoint.lstrip('/')}"
        logger.info(f"📡 GET {url}")
        response = requests.get(url, headers=self.headers)
        try:
            return response.json()
        except Exception:
            logger.error(f"❌ Failed to decode JSON from {url}: {response.text}")
            raise
