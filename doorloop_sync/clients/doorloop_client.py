import os
import logging
import time
import requests
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class DoorLoopClient:
    """A client for interacting with the DoorLoop API with pagination, retry, and validation logic."""

    def __init__(self):
        """Initializes the DoorLoop client, validating necessary environment variables."""
        self.api_key = os.getenv('DOORLOOP_API_KEY')
        self.base_url = os.getenv('DOORLOOP_API_BASE_URL')

        if not self.api_key:
            raise ValueError("DOORLOOP_API_KEY environment variable not set.")
        if not self.base_url:
            raise ValueError("DOORLOOP_API_BASE_URL environment variable not set.")

        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        self.session = requests.Session()
        self.max_retries = 5
        self.retry_delay = 3  # seconds
        self.max_pages = 10000  # safety net for pagination loops

        logger.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}")

    def _make_request(self, method, endpoint, **kwargs):
        """
        Internal method to make a request to the DoorLoop API with retry logic.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST').
            endpoint (str): API endpoint path (e.g., '/api/accounts').
            **kwargs: Additional arguments to pass to the requests call.

        Returns:
            requests.Response: The response object from the API call.
        """
        # Normalize endpoint
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        if not endpoint.startswith("/api/"):
            endpoint = f"/api{endpoint}"

        full_url = urljoin(self.base_url, endpoint)
        logger.info(f"📡 {method.upper()} {full_url}")

        attempt = 0
        while attempt < self.max_retries:
            try:
                response = self.session.request(method, full_url, headers=self.headers, timeout=15, **kwargs)
                if response.status_code == 429:
                    logger.warning(f"⏳ Rate limited on attempt {attempt+1}/{self.max_retries}. Retrying in {self.retry_delay} sec...")
                    time.sleep(self.retry_delay)
                    attempt += 1
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                logger.error(f"❌ HTTP Error for {full_url}: {e.response.status_code} {e.response.reason}")
                logger.error(f"Response Body: {e.response.text}")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request failed: {e}")
                time.sleep(self.retry_delay)
                attempt += 1
        raise RuntimeError(f"❌ Failed after {self.max_retries} attempts to call {full_url}")

    def get_all(self, endpoint, params=None):
        """
        Retrieves all pages of data from a paginated DoorLoop endpoint.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict, optional): Initial query parameters. Defaults to None.

        Returns:
            list: A list of all records fetched from the endpoint.
        """
        if params is None:
            params = {}

        all_records = []
        page = 1

        while page <= self.max_pages:
            params['page'] = page
            response = self._make_request('GET', endpoint, params=params)

            if response.headers.get('Content-Type', '').startswith('text/html'):
                logger.error(f"❌ Received HTML instead of JSON from {endpoint}. This is likely an auth or URL error.")
                logger.error(f"HTML Preview:\n{response.text[:1000]}")
                raise ValueError("Expected JSON response but got HTML.")

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"❌ Failed to decode JSON from {endpoint}. Response text: {response.text}")
                raise

            records = data.get('data', data)

            if not isinstance(records, list):
                logger.warning(f"⚠️ Unexpected response format at page {page}: {type(records)}")
                break

            if not records:
                logger.info(f"📭 No more records at page {page}. Ending pagination.")
                break

            all_records.extend(records)
            logger.info(f"   Fetched page {page}, got {len(records)} records. Total: {len(all_records)}")

            if 'meta' in data and not data['meta'].get('next_page_url'):
                break

            page += 1

        if page >= self.max_pages:
            logger.warning(f"⚠️ Hit pagination safety limit of {self.max_pages} pages. Data may be incomplete.")

        return all_records
