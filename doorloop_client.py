
import os
import requests

class DoorLoopClient:
    def __init__(self):
        self.api_key = os.getenv("DOORLOOP_API_KEY")
        self.base_url = os.getenv("DOORLOOP_API_BASE_URL")
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    def fetch_properties_from_supabase(self):
        print("🔄 Fetching properties from Supabase (raw table)...")
        url = f"{self.supabase_url}/rest/v1/doorloop_raw_properties"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def fetch_from_doorloop(self, endpoint):
        print(f"🌐 Fetching from DoorLoop API: {endpoint}")
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
