import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_raw_tables(cursor):
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'doorloop_raw_%';
    """)
    return [row[0] for row in cursor.fetchall()]

def get_column_names(cursor, table_name):
    cursor.execute(f"""
        SELECT jsonb_object_keys(data) AS key
        FROM {table_name}
        LIMIT 100
    """)
    return list(set(row[0] for row in cursor.fetchall() if row[0]))

def generate_view_sql(table_name, column_names):
    view_name = table_name.replace("raw", "normalized")
    select_parts = []

    # Add ID + Created fields
    select_parts.append("id")
    if "createdAt" in column_names:
        select_parts.append("data->>'createdAt' AS created_at")
    if "updatedAt" in column_names:
        select_parts.append("data->>'updatedAt' AS updated_at")

    for col in column_names:
        if col == "id" or col in ["createdAt", "updatedAt"]:
            continue
        if col == "name":
            select_parts.append("data->>'name' AS name")
        elif col == "data":
            continue  # Avoid aliasing full `data` object
        elif col == "inserted_at" and "createdAt" in column_names:
            select_parts.append("data->>'createdAt' AS source_endpoint")
        else:
            select_parts.append(f"data->>'{col}' AS {col}")

    select_clause = ",\n    ".join(select_parts)

    return f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
        {select_clause}
    FROM {table_name};
    """

def main():
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL is not set.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    logger.info("🔍 Starting view generation from raw tables...")
    try:
        raw_tables = get_raw_tables(cursor)
        for table in raw_tables:
            column_names = get_column_names(cursor, table)
            sql = generate_view_sql(table, column_names)
            try:
                cursor.execute(sql)
                conn.commit()
                logger.info(f"✅ View created: {table.replace('raw', 'normalized')}")
            except Exception as e:
                logger.warning(f"⚠️  Could not create view for {table}: {e}")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
