import logging
import requests

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        logger.info("✅ DoorLoopClient initialized.")

    def get_all(self, endpoint: str) -> list:
        url = f"{self.base_url}/v1/{endpoint}"
        logger.info(f"📡 Fetching all records from {endpoint}...")

        results = []
        page = 1
        while True:
            response = self.session.get(url, params={"page": page, "limit": 100})
            response.raise_for_status()
            data = response.json()

            if not data:
                break
            results.extend(data)
            page += 1

        return results
