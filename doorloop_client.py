import logging
import requests

logger = logging.getLogger(__name__)

class DoorLoopClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": api_key,
            "Content-Type": "application/json",
        }
        logger.info(f"✅ DoorLoopClient initialized with BASE URL: {self.base_url}")

    def fetch_all(self, endpoint, page_size=100):
        all_data = []
        page_number = 1

        while True:
            url = f"{self.base_url}/{endpoint}?pageNumber={page_number}&pageSize={page_size}"
            logger.debug(f"📤 Fetching URL: {url}")
            response = requests.get(url, headers=self.headers)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logger.error(f"❌ Error fetching from {url}: {e}")
                raise

            page_data = response.json()
            if not page_data:
                break

            all_data.extend(page_data)
            if len(page_data) < page_size:
                break

            page_number += 1

        logger.info(f"✅ Fetched {len(all_data)} records from /{endpoint}")
        return all_data
