import requests
import logging
import time # Import the time library

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DoorLoopClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/') # Ensure no trailing slash
        
        # Prepend "Bearer " to the API key if it's not already there 
        if not api_key.lower().startswith('bearer '):
            self.headers = {"Authorization": f"Bearer {api_key}"}
        else:
            self.headers = {"Authorization": api_key} # Use as is if already has "Bearer "
            
        logging.info(f"✅ DoorLoopClient initialized with BASE URL: {self.base_url}/")

    def fetch_all(self, endpoint: str, page_size: int = 100):
        all_data = []
        page_number = 1
        
        logging.info(f"Fetching data from endpoint: /{endpoint}")
        
        while True:
            url = f"{self.base_url}/{endpoint}?pageNumber={page_number}&pageSize={page_size}"
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                
                json_data = response.json()
                data = json_data.get('data', [])
                total_records = json_data.get('total', 0)

                if not data:
                    break  # No more data

                all_data.extend(data)
                
                logging.info(f"  - Fetched page {page_number} ({len(data)} records). Total fetched: {len(all_data)}/{total_records}")

                if len(all_data) >= total_records:
                    break  # All records fetched
                    
                page_number += 1
                
                # Add a delay to avoid hitting API rate limits
                time.sleep(0.5)
            
            except requests.exceptions.HTTPError as http_err:
                logging.error(f"❌ Error fetching from {url}: {http_err}")
                raise  # Re-raise the HTTPError for main script to catch
            except requests.exceptions.ConnectionError as conn_err:
                logging.error(f"❌ Connection Error fetching from {url}: {conn_err}")
                raise
            except requests.exceptions.Timeout as timeout_err:
                logging.error(f"❌ Timeout Error fetching from {url}: {timeout_err}")
                raise
            except requests.exceptions.RequestException as req_err:
                logging.error(f"❌ Request Error fetching from {url}: {req_err}")
                raise
            except Exception as e:
                logging.error(f"❌ An unexpected error occurred while fetching from {url}: {e}")
                raise

        logging.info(f"✅ Finished fetching from /{endpoint}. Total records: {len(all_data)}")
        return all_data