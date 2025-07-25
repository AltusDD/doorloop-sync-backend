import os
import pg8000
from pathlib import Path

VALIDATION_QUERIES = [
    "sql/audit/validate_record_counts.sql",
    "sql/audit/validate_foreign_keys.sql",
    "sql/audit/check_normalized_views.sql",
    "sql/audit/check_full_views.sql"
]

def run_sql(sql_text, conn):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
        print("✅ PASSED")
    except Exception as e:
        print("❌ FAILED")
        print(str(e))
        exit(1)

def main():
    db_url = os.getenv("SUPABASE_URL")
    db_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not db_url or not db_key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment")
        exit(1)

    conn = pg8000.connect(
        user="postgres",
        password=db_key,
        host=db_url.replace("https://", "").split(".")[0] + ".db.supabase.co",
        database="postgres",
        port=5432,
        ssl_context=True
    )

    root = Path(__file__).resolve().parent.parent
    for query_path in VALIDATION_QUERIES:
        full_path = root / query_path
        print(f"🔍 Running {query_path}...")
        sql = full_path.read_text()
        run_sql(sql, conn)

    print("🎉 ALL VALIDATIONS PASSED")

if __name__ == "__main__":
    main()
