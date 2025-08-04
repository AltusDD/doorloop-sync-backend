import os
import time
import requests
import logging

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self):
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL")
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        if not self.base_url or not self.api_key:
            raise ValueError("Missing DoorLoop API credentials in environment variables.")

    def get_all(self, endpoint, params=None):
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        all_data = []
        page = 1

        while True:
            query = params.copy() if params else {}
            query.update({"page": page, "limit": 100})

            logger.info(f"📡 GET {url} page={page}")
            response = requests.get(url, headers=headers, params=query)

            if response.status_code != 200:
                raise Exception(f"Failed to fetch {endpoint}: {response.status_code} - {response.text}")

            try:
                data = response.json()
            except Exception as e:
                logger.error(f"❌ Failed to decode JSON from {url}: {response.text}")
                raise

            if isinstance(data, dict) and "data" in data:
                records = data["data"]
            elif isinstance(data, list):
                records = data
            else:
                raise ValueError(f"Unexpected API response format for endpoint: {endpoint}")

            if not records:
                break

            all_data.extend(records)
            page += 1
            time.sleep(0.2)

        return all_data