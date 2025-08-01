import os
import requests

SQL_PROXY_URL = os.getenv("SQL_PROXY_URL")  # e.g., https://<project>.supabase.co/functions/v1/sql-proxy
SQL_PROXY_SECRET = os.getenv("SQL_PROXY_SECRET")

def deploy_sql_view(file_path):
    with open(file_path, 'r') as f:
        sql_content = f.read()

    response = requests.post(
        SQL_PROXY_URL,
        headers={
            "Authorization": f"Bearer {SQL_PROXY_SECRET}",
            "Content-Type": "application/json"
        },
        json={
            "sql_file": os.path.basename(file_path),
            "sql_content": sql_content
        }
    )

    if response.ok:
        print(f"✅ Success: {os.path.basename(file_path)}")
    else:
        print(f"❌ Error: {os.path.basename(file_path)} → {response.status_code}")
        print(response.text)

def main():
    sql_folder = "normalized_sql"
    for filename in os.listdir(sql_folder):
        if filename.endswith(".sql"):
            deploy_sql_view(os.path.join(sql_folder, filename))

if __name__ == "__main__":
    main()