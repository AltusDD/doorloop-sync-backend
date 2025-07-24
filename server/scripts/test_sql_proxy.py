import os
import requests
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True, help="Path to SQL file")
args = parser.parse_args()

SQL_PROXY_URL = os.getenv("SQL_PROXY_URL")
SQL_PROXY_SECRET = os.getenv("SQL_PROXY_SECRET")

with open(args.file, "r") as f:
    sql_content = f.read()

response = requests.post(
    SQL_PROXY_URL,
    headers={
        "Authorization": f"Bearer {SQL_PROXY_SECRET}",
        "Content-Type": "application/json"
    },
    json={
        "sql_file": os.path.basename(args.file),
        "sql_content": sql_content
    }
)

print(response.status_code)
print(response.json())