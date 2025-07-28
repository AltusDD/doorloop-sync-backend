import os
import requests
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        """
        Initializes the DoorLoop API client.
        Args:
            api_key (str): Your DoorLoop API key
            base_url (str): DoorLoop base API URL (e.g. https://api.doorloop.com/v1)
        """
        self.api_key = api_key.strip()
        self.base_url = base_url.strip()

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        if not self.base_url:
            raise ValueError("DOORLOOP_API_BASE_URL must be provided to DoorLoopClient.")
        if "app.doorloop.com" in self.base_url and not self.base_url.endswith("/api"):
            logger.warning("⚠️ BASE_URL might be incorrect. Use 'https://app.doorloop.com/api' or 'https://api.doorloop.com/v1'")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.last_api_call_time = 0
        logger.info(f"📌 DoorLoopClient initialized with base URL: {self.base_url}")

    def fetch_all(self, endpoint: str, params=None):
        """
        Fetches all paginated records from a DoorLoop endpoint.

        Args:
            endpoint (str): DoorLoop API endpoint, e.g., "leases", "properties"
            params (dict): Additional query parameters

        Returns:
            List of all results across all pages
        """
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        results = []
        page = 1
        RATE_LIMIT_DELAY = 0.1

        while True:
            # Enforce rate limit
            current_time = time.time()
            elapsed_since_last_call = current_time - self.last_api_call_time
            if elapsed_since_last_call < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - elapsed_since_last_call)
            self.last_api_call_time = time.time()

            full_params = {"page_number": page, "page_size": 100}
            if params:
                full_params.update(params)

            response = requests.get(url, headers=self.headers, params=full_params)
            if response.status_code != 200:
                logger.error(f"❌ API call failed: {response.status_code} {response.text}")
                break

            data = response.json()
            if not data.get("data"):
                break

            results.extend(data["data"])
            if not data.get("hasMore", False):
                break

            page += 1

        logger.info(f"✅ Total records fetched from {endpoint}: {len(results)}")
        return results
