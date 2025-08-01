import requests
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        if not api_key.lower().startswith('bearer '):
            self.headers = {"Authorization": f"Bearer {api_key}"}
        else:
            self.headers = {"Authorization": api_key}
        logging.info("✅ DoorLoopClient initialized.")

    def get_all(self, endpoint: str, limit: int = 100):
        """
        Fetches all records from a specified DoorLoop API endpoint with pagination.
        """
        all_data = []
        page_number = 1
        logging.info(f"📡 Fetching all records from {endpoint}...")
        
        while True:
            # --- CORRECTED LINE ---
            # The URL now uses the correct parameter names: 'pageNumber' and 'pageSize'
            params = {'pageNumber': page_number, 'pageSize': limit}
            url = f"{self.base_url}/{endpoint}"
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                
                json_data = response.json()
                data = json_data.get('data', [])
                
                if not data:
                    break  # No more data to fetch

                all_data.extend(data)
                
                # Check if we have fetched all records
                # This logic assumes the API might not return a 'total' count,
                # so we stop when a page returns fewer records than the page size.
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
