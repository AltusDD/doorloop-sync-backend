
import os
import sys
import logging
import psycopg2

def get_db_connection():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise EnvironmentError("Missing DATABASE_URL environment variable")
    return psycopg2.connect(dsn)

def get_columns(table, conn):
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{table}'
    ORDER BY ordinal_position;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [f'"{row[0]}"' for row in cur.fetchall()]

def create_view(table, conn):
    view = table.replace("doorloop_raw_", "doorloop_normalized_")
    columns = get_columns(table, conn)
    if not columns:
        logger.warning(f"⚠️ Skipping {table}, no columns found.")
        return

    sql = f"""
    CREATE OR REPLACE VIEW {view} AS
    SELECT {', '.join(columns)} FROM {table};
    """

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            logger.info(f"✅ View created: {view}")
    except Exception as e:
        logger.error(f"❌ Error creating view for {table}: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    global logger
    logger = logging.getLogger(__name__)
    logger.info("🔍 Starting view generation from raw tables...")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public' AND tablename LIKE 'doorloop_raw_%'
                    ORDER BY tablename;
                """)
                tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                logger.info(f"🔄 Processing {table}...")
                create_view(table, conn)

    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
