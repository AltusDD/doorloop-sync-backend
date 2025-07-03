
import os
import requests
import logging

DOORLOOP_BASE_URL = os.getenv("DOORLOOP_BASE_URL")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

if not DOORLOOP_BASE_URL:
    raise ValueError("❌ Environment variable DOORLOOP_BASE_URL is not set.")
if not DOORLOOP_API_KEY:
    raise ValueError("❌ Environment variable DOORLOOP_API_KEY is not set.")

logging.basicConfig(level=logging.INFO)
logging.info(f"📡 Using DoorLoop Base URL: {DOORLOOP_BASE_URL}")

def fetch_all_records(endpoint):
    all_records = []
    page_number = 1
    page_size = 100
    headers = {"Authorization": f"Bearer {DOORLOOP_API_KEY}"}

    while True:
        url = f"{DOORLOOP_BASE_URL}/{endpoint}?page_number={page_number}&page_size={page_size}"
        logging.info(f"📤 Fetching: {url}")
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logging.error(f"❌ Failed to fetch from {endpoint}: {response.text}")
            raise Exception(f"Failed to fetch from {endpoint}: {response.text}")

        data = response.json()
        if not data:
            break
        all_records.extend(data)
        if len(data) < page_size:
            break
        page_number += 1

    logging.info(f"✅ Retrieved {len(all_records)} records from {endpoint}")
    return all_records
