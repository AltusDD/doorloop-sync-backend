import requests
import os
import time
import logging

DOORLOOP_API_KEY = os.environ.get("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.environ.get("DOORLOOP_API_BASE_URL")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}"
}

logger = logging.getLogger(__name__)
RATE_LIMIT_DELAY = 0.1
LAST_API_CALL_TIME = 0

def fetch_all(endpoint):
    global LAST_API_CALL_TIME
    results = []
    page = 1

    while True:
        elapsed = time.time() - LAST_API_CALL_TIME
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)

        url = f"{DOORLOOP_API_BASE_URL}/{endpoint}?page_number={page}&page_size=100"
        response = requests.get(url, headers=HEADERS)
        LAST_API_CALL_TIME = time.time()

        if response.status_code == 429:
            retry = int(response.headers.get("Retry-After", 5))
            logger.warning(f"Rate limit hit. Sleeping for {retry}s.")
            time.sleep(retry)
            continue

        if response.status_code == 404:
            logger.warning(f"{endpoint} not found.")
            break

        if response.status_code != 200:
            logger.error(f"Error {response.status_code} for {endpoint}: {response.text}")
            break

        data = response.json().get("data", [])
        if not data:
            break

        results.extend(data)
        page += 1

    return results
