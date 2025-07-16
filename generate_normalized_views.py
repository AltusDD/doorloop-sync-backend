import os
import psycopg2
import logging
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")

    url = urlparse(database_url)

    return psycopg2.connect(
        dbname=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port,
        sslmode="require"
    )

def generate_view_sql(table_name):
    return f"""
    CREATE OR REPLACE VIEW {table_name.replace('doorloop_raw_', 'view_')} AS
    SELECT *
    FROM {table_name};
    """

def main():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name LIKE 'doorloop_raw_%';
    """)
    tables = [row[0] for row in cursor.fetchall()]
    logger.info(f"✅ Found {len(tables)} raw tables to process")

    for table in tables:
        view_sql = generate_view_sql(table)
        logger.info(f"📤 Creating view for {table}")
        try:
            cursor.execute(view_sql)
            logger.info(f"✅ View created for {table}")
        except Exception as e:
            logger.error(f"❌ Failed to create view for {table}: {e}")
            conn.rollback()

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()