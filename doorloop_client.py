
import requests
import os
import logging

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

def fetch_data_from_doorloop(endpoint: str, base_url: str, api_key: str) -> list:
    if not base_url or not api_key:
        _logger.error("❌ Missing DoorLoop API configuration in environment variables.")
        return []

    headers = {
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json"
    }

    results = []
    page = 1
    page_size = 100

    while True:
        url = f"{base_url}{endpoint}?page_number={page}&page_size={page_size}"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            if "application/json" not in response.headers.get("Content-Type", ""):
                raise ValueError(f"Non-JSON response from DoorLoop API for {endpoint}")

            data = response.json()
            if isinstance(data, dict) and "data" in data:
                batch = data["data"]
            else:
                batch = data

            if not batch:
                break

            results.extend(batch)
            page += 1
        except requests.HTTPError as http_err:
            if response.status_code == 405:
                _logger.warning(f"405 Method Not Allowed: {url}")
                break
            _logger.error(f"HTTPError fetching {endpoint}: {http_err}")
            break
        except ValueError as val_err:
            _logger.error(f"ValueError for {endpoint}: {val_err}")
            break
        except Exception as ex:
            _logger.error(f"Unexpected error for {endpoint}: {ex}")
            break

    return results
