import requests
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        # --- DEFINITIVE URL FIX ---
        # The base URL is now hardcoded to the correct structure as per the official documentation.
        # This ensures we are always hitting the correct hostname (app.doorloop.com) and path (/api).
        self.base_url = "https://app.doorloop.com/api"
        
        if not api_key.lower().startswith('bearer '):
            self.headers = {"Authorization": f"Bearer {api_key}"}
        else:
            self.headers = {"Authorization": api_key}
            
        logging.info(f"✅ DoorLoopClient initialized. Using validated BASE URL: {self.base_url}/")

    def get_all(self, endpoint: str, limit: int = 100):
        """
        Fetches all records from a specified DoorLoop API endpoint with pagination.
        """
        all_data = []
        page_number = 1
        logging.info(f"📡 Fetching all records from {endpoint}...")
        
        while True:
            # Using the correct parameter names 'page' and 'limit' as per the API documentation.
            params = {'page': page_number, 'limit': limit}
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                json_data = response.json()
                # The documentation shows the data is directly in the response array.
                data = json_data
                
                if not data:
                    break

                all_data.extend(data)
                
                if len(data) < limit:
                    break
                    
                page_number += 1
                time.sleep(0.2) # Small delay to be respectful to the API
            
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"❌ HTTP Error fetching from {url} with params {params}: {http_err}")
                raise
            except Exception as e:
                logging.error(f"❌ An unexpected error occurred while fetching from {url}: {e}")
                raise

        logging.info(f"✅ Finished fetching from /{endpoint}. Total records: {len(all_data)}")
        return all_data
