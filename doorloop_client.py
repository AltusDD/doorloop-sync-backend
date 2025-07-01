# doorloop_client.py
import os
import requests
import time
import json
import logging # Import logging
from typing import List, Dict, Any, Optional

# Initialize logging for this module
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO) # Set level for this module

# The function that fetches all data from a DoorLoop API endpoint
def fetch_all_doorloop_records(endpoint: str, base_url: str, token: str) -> List[Dict[str, Any]]:
    if not token:
        _logger.error("DoorLoop API token is missing in fetch_all_doorloop_records.")
        raise ValueError("DoorLoop API token is missing.")
    if not base_url:
        _logger.error("DoorLoop API base URL is missing in fetch_all_doorloop_records.")
        raise ValueError("DoorLoop API base URL is missing.")
    if not endpoint:
        _logger.error("DoorLoop API endpoint is missing in fetch_all_doorloop_records.")
        raise ValueError("DoorLoop API endpoint is missing.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    all_data = []
    page_number = 1
    page_size = 100 

    _logger.info(f"\n--- DEBUG_FETCH: Starting paginated fetch for {endpoint} ---")
    while True:
        url = f"{base_url}{endpoint}?page_number={page_number}&page_size={page_size}"
        _logger.info(f"DEBUG_FETCH: Requesting URL: {url}") # Log the exact URL being called

        try:
            response = requests.get(url, headers=headers, timeout=30)

            _logger.info(f"DEBUG_FETCH: Response Status Code: {response.status_code}")
            _logger.info(f"DEBUG_FETCH: Response Content-Type: {response.headers.get('Content-Type')}")
            if response.status_code != 200 or not response.headers.get('Content-Type', '').startswith('application/json'):
                _logger.warning(f"DEBUG_FETCH: Non-JSON/Non-200 Response Body (first 500 chars): {response.text[:500]}...")

            response.raise_for_status() # This will raise HTTPError for 4xx/5xx responses

            data = response.json()

            current_page_data = data.get('data', data) if isinstance(data, dict) else data

            if not isinstance(current_page_data, list):
                _logger.warning(f"WARN_FETCH: Unexpected data format for {endpoint} on page {page_number}: Not a list. Data: {current_page_data}")
                break 

            if not current_page_data: # If no data on this page, it's the end.
                _logger.info(f"DEBUG_FETCH: No data on page {page_number}. Ending fetch.")
                break

            all_data.extend(current_page_data)

            if isinstance(data, dict) and 'total' in data and 'count' in data:
                if len(all_data) >= data['total']:
                    _logger.info(f"DEBUG_FETCH: {endpoint} Total records from API metadata: {data['total']}. All pages fetched.")
                    break
            elif len(current_page_data) < page_size:
                _logger.info(f"DEBUG_FETCH: Less than page_size on page {page_number}. Ending fetch.")
                break

            page_number += 1
        except requests.exceptions.RequestException as e:
            _logger.error(f"ERROR_FETCH: Request failed for {url}: {e}")
            raise 
        except Exception as e:
            _logger.error(f"ERROR_FETCH: Unexpected error for {url}: {e}")
            raise 
    _logger.info(f"--- DEBUG_FETCH: Finished fetching {len(all_data)} records from {endpoint}. ---")
    return all_data