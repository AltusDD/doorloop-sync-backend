
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_BASE_URL = os.getenv("DOORLOOP_BASE_URL", "https://api.doorloop.com/v1")

HEADERS = {
    "Authorization": f"Bearer {DOORLOOP_API_KEY}",
    "Content-Type": "application/json"
}

async def fetch_all_entities(endpoint):
    records = []
    page = 1
    while True:
        url = f"{DOORLOOP_BASE_URL}{endpoint}?page={page}&limit=100"
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()

        if not data or len(data) == 0:
            break

        records.extend(data)
        if len(data) < 100:
            break
        page += 1

    return records
