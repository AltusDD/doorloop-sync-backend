import os
import psycopg2

def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(database_url)

def execute_sql(sql):
    print(f"Executing SQL:\n{sql}")
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            conn.commit()

def load_sql_file(path):
    print(f"📂 Loading SQL file: {path}")
    with open(path, "r") as f:
        return f.read()

def main():
    print("🚀 Starting normalized views generation...")

    sql_dir = os.path.join(os.getcwd(), "normalized_sql")
    if not os.path.isdir(sql_dir):
        raise Exception(f"❌ normalized_sql directory not found at: {sql_dir}")

    sql_files = sorted(f for f in os.listdir(sql_dir) if f.endswith(".sql"))

    if not sql_files:
        raise Exception(f"❌ No .sql files found in: {sql_dir}")

    for filename in sql_files:
        file_path = os.path.join(sql_dir, filename)
        sql = load_sql_file(file_path)
        try:
            execute_sql(sql)
            print(f"✅ Executed: {filename}")
        except Exception as e:
            print(f"❌ Error executing {filename}: {e}")

    print("✅ All views processed.")

if __name__ == "__main__":
    main()
