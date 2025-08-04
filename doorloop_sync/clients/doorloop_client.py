
import requests
import logging

class DoorLoopClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        logging.info(f"✅ DoorLoopClient initialized with base_url: {self.base_url}")

    def get_all(self, endpoint_path):
        full_url = f"{self.base_url}/{endpoint_path.lstrip('/')}"
        logging.info(f"📡 GET {full_url}")
        try:
            response = requests.get(full_url, headers=self.headers)
            if "text/html" in response.headers.get("Content-Type", ""):
                logging.error(f"❌ HTML page received instead of JSON. Possible invalid endpoint or API key. URL: {full_url}")
                raise ValueError("HTML response detected. Check API base URL and key.")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request failed: {e}")
            raise
        except ValueError as e:
            logging.error(f"❌ Value error: {e}")
            raise
