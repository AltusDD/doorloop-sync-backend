# doorloop_client.py
import os
import requests
import time
import json
from typing import List, Dict, Any, Optional

# The function that fetches all data from a DoorLoop API endpoint
def fetch_all_doorloop_records(endpoint: str, base_url: str, token: str) -> List[Dict[str, Any]]:
    if not token:
        raise ValueError("DoorLoop API token is missing in fetch_all_doorloop_records.")
    if not base_url:
        raise ValueError("DoorLoop API base URL is missing in fetch_all_doorloop_records.")
    if not endpoint:
        raise ValueError("DoorLoop API endpoint is missing in fetch_all_doorloop_records.")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    all_data = []
    page = 1
    page_size = 100 

    print(f"\n--- DEBUG_FETCH: Starting paginated fetch for {endpoint} ---")
    while True:
        # --- FIX: Use the correct base_url directly, and append endpoint ---
        # The base_url should be 'https://app.doorloop.com/api'
        # The endpoint will be '/properties', '/units', etc.
        url = f"{base_url}{endpoint}?page_number={page}&page_size={page_size}" 
        print(f"DEBUG_FETCH: Requesting URL: {url}") # Log the exact URL being called

        try:
            response = requests.get(url, headers=headers, timeout=30)

            print(f"DEBUG_FETCH: Response Status Code: {response.status_code}")
            print(f"DEBUG_FETCH: Response Content-Type: {response.headers.get('Content-Type')}")
            print(f"DEBUG_FETCH: Response Body (first 500 chars): {response.text[:500]}...")

            response.raise_for_status() # This will raise HTTPError for 4xx/5xx responses

            data = response.json()

            current_page_data = data.get('data', data) if isinstance(data, dict) else data

            if not isinstance(current_page_data, list):
                print(f"WARN_FETCH: Unexpected data format for {endpoint} on page {page_number}: Not a list. Data: {current_page_data}")
                break 

            if not current_page_data: # If no data on this page, it's the end.
                print(f"DEBUG_FETCH: No data on page {page_number}. Ending fetch.")
                break

            all_data.extend(current_page_data)

            if isinstance(data, dict) and 'total' in data and 'count' in data:
                if len(all_data) >= data['total']:
                    print(f"DEBUG_FETCH: {endpoint} Total records from API metadata: {data['total']}. All pages fetched.")
                    break
            elif len(current_page_data) < page_size:
                print(f"DEBUG_FETCH: Less than page_size on page {page_number}. Ending fetch.")
                break

            page_number += 1
        except requests.exceptions.RequestException as e:
            print(f"ERROR_FETCH: Request failed for {url}: {e}")
            break
        except Exception as e:
            print(f"ERROR_FETCH: Unexpected error for {url}: {e}")
            break
    print(f"--- DEBUG_FETCH: Finished fetching {len(all_data)} records from {endpoint}. ---")
    return all_data