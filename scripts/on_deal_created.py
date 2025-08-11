# on_deal_created.py
# 🪝 Azure Function to create Dropbox folders when a new deal is inserted

import logging
import os
import requests
import azure.functions as func

DROPBOX_API_URL = "https://api.dropboxapi.com/2/files/create_folder_v2"


def main(event: func.EventGridEvent):
    """Triggered when a new deal is inserted into the Supabase 'deals' table."""
    deal = event.get_json()["record"]["new"]
    deal_name = deal["name"].lower().replace(" ", "-")
    folder_path = f"/04_Deal_Room/{deal_name}-{deal['id']}"

    headers = {
        "Authorization": f"Bearer {os.environ['DROPBOX_ACCESS_TOKEN']}",
        "Content-Type": "application/json",
    }
    body = {"path": folder_path, "autorename": False}

    resp = requests.post(DROPBOX_API_URL, headers=headers, json=body)
    if resp.status_code != 200:
        logging.error("Dropbox folder creation failed: %s %s", resp.status_code, resp.text)
    else:
        logging.info("Created Dropbox folder at %s", folder_path)
