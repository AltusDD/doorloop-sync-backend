import os
import logging
import time
import requests
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class DoorLoopClient:
    """Client for DoorLoop API with pagination, retries, access control, and write mode security."""

    def __init__(self, write_mode=False):
        self.api_key = os.getenv('DOORLOOP_API_KEY')
        self.base_url = os.getenv('DOORLOOP_API_BASE_URL')
        self.user_role = os.getenv('ALTUS_USER_ROLE', 'viewer').lower()
        self.write_mode = write_mode

        if not self.api_key or not self.base_url:
            raise ValueError("Missing required environment variables: DOORLOOP_API_KEY or DOORLOOP_API_BASE_URL.")

        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

        self.session = requests.Session()
        self.max_retries = 5
        self.retry_base_delay = 3  # seconds
        self.max_pages = 10000

        logger.info(f"✅ DoorLoopClient initialized. Role: {self.user_role}. Write mode: {self.write_mode}. Using URL: {self.base_url}")

    def _validate_write_permission(self, method):
        if method.upper() in ['POST', 'PATCH', 'DELETE'] and not self.write_mode:
            raise PermissionError(f"🚫 Write operation blocked. write_mode=False")
        if self.user_role not in ['admin', 'accounting']:
            raise PermissionError(f"🚫 User role '{self.user_role}' not permitted to perform write operations.")
        logger.info(f"✅ Write access granted for role '{self.user_role}' on method {method}")

    def _make_request(self, method, endpoint, **kwargs):
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        if not endpoint.startswith("/api/"):
            endpoint = f"/api{endpoint}"

        full_url = urljoin(self.base_url, endpoint)
        logger.info(f"📡 {method.upper()} {full_url}")

        if method.upper() in ['POST', 'PATCH', 'DELETE']:
            self._validate_write_permission(method)

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(method, full_url, headers=self.headers, timeout=15, **kwargs)
                if response.status_code == 429:
                    delay = self.retry_base_delay * attempt
                    logger.warning(f"⏳ Rate limited. Attempt {attempt}/{self.max_retries}. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                logger.error(f"❌ HTTP Error {response.status_code} for {full_url}: {e}")
                logger.error(f"🔍 Response Body: {getattr(e.response, 'text', '')}")
                raise
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request failed: {e}")
                time.sleep(self.retry_base_delay * attempt)
        raise RuntimeError(f"❌ Failed after {self.max_retries} attempts: {method} {full_url}")

    def get_all(self, endpoint, params=None):
        if params is None:
            params = {}

        all_records = []
        page = 1

        while page <= self.max_pages:
            params['page'] = page
            response = self._make_request('GET', endpoint, params=params)

            if response.headers.get('Content-Type', '').startswith('text/html'):
                logger.error(f"❌ HTML response from {endpoint}. Likely a URL or auth error.")
                logger.error(f"🔍 HTML Snippet:\n{response.text[:1000]}")
                raise ValueError("Expected JSON response but got HTML.")

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                logger.error(f"❌ JSON decode failed for {endpoint}. Text:\n{response.text}")
                raise

            records = data.get('data', data)
            if not isinstance(records, list):
                logger.warning(f"⚠️ Unexpected format at page {page}: {type(records)}")
                break
            if not records:
                logger.info(f"📭 No more records at page {page}. Ending pagination.")
                break

            all_records.extend(records)
            logger.info(f"   Fetched page {page}, {len(records)} records. Total: {len(all_records)}")

            if 'meta' in data and not data['meta'].get('next_page_url'):
                break
            page += 1

        if page >= self.max_pages:
            logger.warning(f"⚠️ Hit page limit of {self.max_pages}. Results may be truncated.")
        return all_records
