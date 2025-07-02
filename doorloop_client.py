
import requests

def fetch_data_from_doorloop(endpoint: str, base_url: str, api_key: str):
    if not base_url or not api_key:
        raise ValueError("Missing DoorLoop API configuration in environment variables.")

    url = f"{base_url.rstrip('/')}{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching from DoorLoop: {e}")
    except ValueError:
        raise ValueError("Invalid JSON response from DoorLoop API.")
