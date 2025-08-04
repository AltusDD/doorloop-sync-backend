import os
import logging
import requests
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class DoorLoopClient:
    """A client for interacting with the DoorLoop API."""

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
        logger.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}")

    def _make_request(self, method, endpoint, **kwargs):
        """
        Internal method to make a request to the DoorLoop API.

        Args:
            method (str): HTTP method (e.g., 'GET', 'POST').
            endpoint (str): API endpoint path (e.g., '/api/accounts').
            **kwargs: Additional arguments to pass to the requests call.

        Returns:
            requests.Response: The response object from the API call.
        """
        # Ensure the endpoint starts with a slash for clean joining
        if not endpoint.startswith('/'):
            endpoint = f'/{endpoint}'
            
        full_url = urljoin(self.base_url, endpoint)
        logger.info(f"📡 {method.upper()} {full_url}")

        try:
            response = requests.request(method, full_url, headers=self.headers, **kwargs)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            return response
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for {full_url}: {e.response.status_code} {e.response.reason}")
            logger.error(f"Response Body: {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {full_url}: {e}")
            raise

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
        
        while True:
            params['page'] = page
            response = self._make_request('GET', endpoint, params=params)

            # Defensive check for HTML response
            if response.headers.get('Content-Type', '').startswith('text/html'):
                logger.error(f"❌ Received HTML instead of JSON from {endpoint}. This is likely an auth or URL error.")
                logger.error(f"Response Preview: {response.text[:500]}...")
                raise requests.exceptions.JSONDecodeError("Expecting JSON, got HTML.", response.text, 0)
            
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"❌ Failed to decode JSON from {endpoint}. Response text: {response.text}")
                raise

            records = data.get('data', data) # Handle both enveloped and non-enveloped responses
            
            if not records:
                break # Exit loop if no more records are returned

            all_records.extend(records)
            
            # Check for pagination metadata to see if we should continue
            if 'meta' in data and data['meta'].get('next_page_url') is None:
                break
                
            # As a fallback if no metadata, stop if we get an empty list
            if not isinstance(records, list) or len(records) == 0:
                break

            page += 1
            logger.info(f"   Fetched page {page-1}, got {len(records)} records. Total: {len(all_records)}")

        return all_records

