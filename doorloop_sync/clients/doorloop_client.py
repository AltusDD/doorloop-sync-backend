
import requests
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        logger.info("✅ DoorLoopClient initialized.")

    def get_all(self, endpoint, params=None):
        logger.info(f"📡 Fetching all records from {endpoint}...")
        results = []
        page = 1
        per_page = 100
        while True:
            query_params = {"page": page, "limit": per_page}
            if params:
                query_params.update(params)
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(url, headers=self.headers, params=query_params)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            results.extend(data)
            if len(data) < per_page:
                break
            page += 1
        logger.info(f"✅ Retrieved {len(results)} records from {endpoint}.")
        return results
