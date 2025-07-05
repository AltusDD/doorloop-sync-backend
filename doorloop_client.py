import os
import requests
import time
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key.strip()
        self.base_url = base_url.strip()

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        if not self.base_url:
            raise ValueError("DOORLOOP_API_BASE_URL must be provided to DoorLoopClient.")
        # Ensure base_url ends with /api if it's the app.doorloop.com domain
        if "app.doorloop.com" in self.base_url and not self.base_url.endswith("/api"):
            logger.warning("⚠️ BASE_URL likely incorrect. Consider using 'https://app.doorloop.com/api' or 'https://api.doorloop.com/v1'")
            # Optionally, you could try to fix it here:
            # self.base_url = self.base_url.rstrip('/') + '/api'

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.last_api_call_time = 0 # Instance variable for rate limiting
        logger.debug(f"📌 Initialized DoorLoopClient with base URL: {self.base_url}")

    def fetch_all(self, endpoint: str, params=None):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        logger.debug(f"📤 Fetching from endpoint: {endpoint}, Full URL: {url}")
        results = []
        page = 1
        RATE_LIMIT_DELAY = 0.1 # seconds per request, adjust as needed based on DoorLoop's limits

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
            logger.debug(f"📄 Request params for page {page}: {full_params}")

            response = requests.get(url, headers=self.headers, params=full_params)
            logger.debug(f"📥 HTTP {response.status_code} from {url}")

            if response.status_code == 429: # Too Many Requests
                retry_after = int(response.headers.get("Retry-After", 5)) # Default 5 seconds
                logger.warning(f"⚠️ Rate limit hit for {endpoint}. Retrying after {retry_after} seconds. Response: {response.text}")
                time.sleep(retry_after)
                continue # Retry the same page

            if not response.ok:
                logger.error(f"❌ Failed to fetch {endpoint}: {response.status_code} - {response.text}")
                raise Exception(f"❌ Failed to fetch {endpoint}: {response.status_code} - {response.text}")

            try:
                page_data = response.json()
            except ValueError:
                # This catches the "Expecting value: line 1 column 1" error for non-JSON responses
                logger.error(
                    f"❌ Failed to parse JSON from {endpoint}. Received non-JSON response.\n"
                    f"Status Code: {response.status_code}\n"
                    f"Headers: {response.headers}\n"
                    f"Raw Response:\n{response.text}"
                )
                # Re-raise the error as it's a critical issue for data fetching
                raise

            # DoorLoop API returns { "total": N, "data": [...] } for some endpoints,
            # but a direct list for others. Handle both cases.
            data_list = page_data.get("data") if isinstance(page_data, dict) and "data" in page_data else page_data

            if not data_list or not isinstance(data_list, list):
                logger.debug("🛑 No more data or malformed response (expected list of dicts)")
                break
            results.extend(data_list)
            logger.debug(f"✅ Fetched {len(data_list)} items from page {page}")

            # Check if it's the last page (less than page_size implies last page)
            if len(data_list) < 100:
                break
            page += 1

        return results

