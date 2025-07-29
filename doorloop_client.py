# doorloop_client.py

import os
import requests
import logging
from urllib.parse import urljoin

logger = logging.getLogger("doorloop_client")

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/v1")

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY environment variable is missing.")
        if not self.base_url.endswith("/"):
            self.base_url += "/"

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        logger.info(f"✅ DoorLoopClient initialized with BASE URL: {self.base_url}")

    def fetch_all(self, endpoint):
        full_url = urljoin(self.base_url, endpoint)
        page_number = 1
        page_size = 100
        all_data = []

        while True:
            try:
                paged_url = f"{full_url}?pageNumber={page_number}&pageSize={page_size}"
                logger.debug(f"🔍 [DEBUG] URL: {paged_url}")
                response = self.session.get(paged_url)
                logger.debug(f"🔍 [DEBUG] Status Code: {response.status_code}")
                logger.debug(f"🔍 [DEBUG] Response Text Preview:\n{response.text[:500]}")

                response.raise_for_status()
                data = response.json()

                if isinstance(data, list):
                    batch = data
                elif isinstance(data, dict) and "data" in data:
                    batch = data["data"]
                else:
                    batch = []

                if not batch:
                    break

                all_data.extend(batch)

                if len(batch) < page_size:
                    break

                page_number += 1

            except Exception as e:
                logger.exception(f"❌ Error fetching from {paged_url}: {e}")
                raise

        return all_data
