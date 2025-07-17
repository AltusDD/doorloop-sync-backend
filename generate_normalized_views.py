import os
import psycopg2


def get_connection():
    # First try DATABASE_URL, then fall back to SUPABASE_DB_URL
    database_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not database_url:
        raise Exception("❌ DATABASE_URL and SUPABASE_DB_URL are not set in environment variables.")
    print(f"🔐 Using database connection string from {'DATABASE_URL' if os.getenv('DATABASE_URL') else 'SUPABASE_DB_URL'}")
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

    # Check both normalized_sql and normalize as valid fallback folders
    valid_dirs = ["normalized_sql", "normalize"]
    sql_dir = next(
        (os.path.join(os.getcwd(), d) for d in valid_dirs if os.path.isdir(os.path.join(os.getcwd(), d))),
        None
    )

    if not sql_dir:
        raise Exception(f"❌ No valid SQL directory found. Checked: {', '.join(valid_dirs)}")

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
