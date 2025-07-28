
import os
import requests
import logging
import time
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DoorLoopClient:
    def __init__(self, api_key: str, base_url: str = "https://api.doorloop.com/v1"):
        if not api_key:
            raise ValueError("DOORLOOP_API_KEY must be provided to DoorLoopClient.")
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        logger.info(f"DoorLoopClient initialized for base URL: {self.base_url}")

    def _call_doorloop_api(self, method: str, endpoint: str, params: Dict[str, Any] = None, json_data: Dict[str, Any] = None, retries: int = 3, initial_delay: int = 1) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        for attempt in range(retries):
            try:
                logger.debug(f"Attempt {attempt+1}/{retries}: {method} {url} with params: {params}")
                response = self.session.request(method, url, params=params, json=json_data, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                if status_code in [429, 500, 502, 503, 504] and attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.warning(f"Transient HTTP Error {status_code} for {endpoint}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Unrecoverable HTTP Error {status_code} for {endpoint}: {e.response.text}")
                    raise
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.warning(f"Network Error for {endpoint}: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Unrecoverable Network Error for {endpoint}: {e}")
                    raise
        raise Exception(f"Failed to call DoorLoop API for {endpoint} after {retries} attempts.")

    def fetch_all(self, endpoint: str, page_size: int = 100) -> List[Dict[str, Any]]:
        all_records = []
        page = 1
        logger.info(f"Fetching all records from DoorLoop endpoint: {endpoint}")
        while True:
            params = {"page": page, "pageSize": page_size}
            try:
                response = self._call_doorloop_api("GET", endpoint, params=params)
                records = response.json()
                if not records:
                    logger.info(f"No more records found for {endpoint} after page {page-1}.")
                    break
                all_records.extend(records)
                logger.info(f"Fetched {len(records)} records from {endpoint} (Page {page}). Total: {len(all_records)}")
                page += 1
            except Exception as e:
                logger.error(f"Error fetching data from {endpoint} at page {page}: {e}", exc_info=True)
                break
        logger.info(f"Finished fetching all records from {endpoint}. Total: {len(all_records)}")
        return all_records
