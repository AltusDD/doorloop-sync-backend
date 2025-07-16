import os
import psycopg2
import logging
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    try:
        dsn = os.environ.get("SUPABASE_DB_URL")
        if not dsn:
            raise ValueError("SUPABASE_DB_URL environment variable is missing.")
        conn = psycopg2.connect(dsn)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

def automate_view_creation(schema, mappings):
    conn = get_db_connection()
    cursor = conn.cursor()
    for raw_table, view_name in mappings.items():
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{raw_table}'")
        columns = [row[0] for row in cursor.fetchall()]
        if not columns:
            logger.warning(f"No columns found for {raw_table}")
            continue
        column_list = ", ".join(columns)
        view_sql = (
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT {column_list} FROM {raw_table};"
        )
        try:
            cursor.execute(view_sql)
            conn.commit()
            logger.info(f"✅ Created view: {view_name}")
        except Exception as e:
            logger.error(f"❌ Failed to create view {view_name}: {e}")
            conn.rollback()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    schema = "public"
    mappings = {
        "doorloop_raw_properties": "doorloop_normalized_properties",
        "doorloop_raw_units": "doorloop_normalized_units",
        # Add more mappings as needed
    }
    automate_view_creation(schema, mappings)
