import os
import requests
import time
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        """
        Initializes the DoorLoop API client.
        API key and base URL are passed as arguments, not read directly from os.getenv here.
        """
        self.api_key = api_key.strip()
        self.base_url = base_url.strip()

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        if not self.base_url:
            raise ValueError("DOORLOOP_API_BASE_URL must be provided to DoorLoopClient.")
        if "app.doorloop.com" in self.base_url and not self.base_url.endswith("/api"):
            logger.warning("⚠️ BASE_URL likely incorrect. Consider using 'https://app.doorloop.com/api' or 'https://api.doorloop.com/v1'")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.last_api_call_time = 0
        logger.debug(f"📌 Initialized DoorLoopClient with base URL: {self.base_url}")

    def fetch_all(self, endpoint: str, params=None):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        logger.debug(f"📤 Fetching from endpoint: {endpoint}, Full URL: {url}")
        results = []
        page = 1
        RATE_LIMIT_DELAY = 0.1

        while True:
            # Enforce rate limit
            current_time = time.time()
            elapsed_since_last_call = current_time - self.last_api_call_time
            if elapsed_since_last_call < RATE_LIMIT_DELAY:
                logger.debug(f"⏱️ Rate limiting active, sleeping for {RATE_LIMIT_DELAY - elapsed_since_last_call:.2f} seconds")
                time.sleep(RATE_LIMIT_DELAY - elapsed_since_last_call)
            self.last_api_call_time = time.time()

            full_params = {"page_number": page, "page_size": 100}
            if params:
                full_params.update(params)

            response = requests.get(url, headers=self.headers, params=full_params)
            if response.status_code != 200:
                logger.error(f"❌ Failed API call: {response.status_code} {response.text}")
                break

            data = response.json()
            if not data.get("data"):
                break

            results.extend(data["data"])
            if not data.get("hasMore", False):
                break

            page += 1
            logger.debug(f"📄 Page {page - 1} fetched, continuing...")

        logger.info(f"✅ Fetched {len(results)} records from {endpoint}")
        return results
