import os
import requests
import logging

logger = logging.getLogger("ETL_Orchestrator")

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get(self, endpoint):
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"DoorLoop API failed: {response.status_code} - {response.text}")
            return None
        try:
            return response.json()
        except Exception:
            logger.error("DoorLoop response was not valid JSON.")
            return None