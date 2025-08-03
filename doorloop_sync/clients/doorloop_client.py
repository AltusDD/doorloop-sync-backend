
import requests
import logging
import time
from urllib.parse import urlparse, urlunparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        # This logic uses robust URL parsing to ensure the base URL is always
        # correctly formatted, preserving the original domain.
        self.base_url = "https://app.doorloop.com/api"
        
        if not api_key.lower().startswith('bearer '):
            self.headers = {"Authorization": f"Bearer {api_key}"}
        else:
            self.headers = {"Authorization": api_key}
            
        logging.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}/")

    def get_all(self, endpoint: str, limit: int = 100):
        """
        Fetches all records from a specified DoorLoop API endpoint with pagination.
        """
        all_data = []
        page_number = 1
        logging.info(f"📡 Fetching all records from {endpoint}...")
        
        while True:
            params = {'page': page_number, 'limit': limit}
            url = f"{self.base_url}/{endpoint.lstrip('/')}"

            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()

                json_data = response.json()
                data = json_data.get('data', [])
                total_records = json_data.get('total', 0)

                if not data:
                    break

                all_data.extend(data)

                logging.info(f"  - Fetched page {page_number} ({len(data)} records). Total fetched so far: {len(all_data)}/{total_records}")

                if total_records > 0 and len(all_data) >= total_records:
                    break

                if len(data) < limit:
                    break  # Stop if we get a partial page, indicating the end

                page_number += 1
                time.sleep(0.2)

            except requests.exceptions.HTTPError as http_err:
                logging.error(f"❌ HTTP Error fetching from {url} with params {params}: {http_err}")
                raise
            except Exception as e:
                logging.error(f"❌ An unexpected error occurred while fetching from {url}: {e}")
                raise

        logging.info(f"✅ Finished fetching from /{endpoint}. Total records: {len(all_data)}")
        return all_data
