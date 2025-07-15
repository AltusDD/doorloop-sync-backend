import psycopg2
from psycopg2 import sql
from psycopg2.errors import SyntaxError, UndefinedColumn, DuplicateColumn
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", 5432)
        )
        logging.info("✅ Connected to database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"❌ Database connection failed: {e}")
        raise

def get_table_columns(cursor, schema_name, table_name):
    try:
        cursor.execute(
            sql.SQL("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position;
            """),
            [schema_name, table_name]
        )
        columns = [row[0] for row in cursor.fetchall()]
        if not columns:
            logging.warning(f"⚠️ No columns found for table {table_name}")
        return columns
    except psycopg2.Error as e:
        logging.error(f"❌ Error fetching columns for {table_name}: {e}")
        return []

def generate_create_view_sql(schema_name, raw_table_name, view_name, columns):
    if not columns:
        return None

    quoted_columns = [sql.Identifier(col) for col in columns]
    select_list = sql.SQL(', ').join(quoted_columns)
    quoted_raw_table = sql.Identifier(schema_name, raw_table_name)
    quoted_view_name = sql.Identifier(schema_name, view_name)

    query = sql.SQL("""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT {select_list}
        FROM {raw_table};
    """).format(
        view_name=quoted_view_name,
        select_list=select_list,
        raw_table=quoted_raw_table
    )

    return query.as_string(get_db_connection())

def execute_sql_statement(conn, sql_statement):
    if not sql_statement:
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_statement)
        conn.commit()
        logging.info("✅ View created successfully.")
        return True
    except (SyntaxError, UndefinedColumn, DuplicateColumn) as e:
        conn.rollback()
        logging.error(f"❌ SQL DDL error:\n{e}")
        return False
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"❌ PostgreSQL error:\n{e}")
        return False
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Unexpected error:\n{e}")
        return False

def automate_view_creation(schema_name, table_mappings):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            for raw_table, view_name in table_mappings.items():
                logging.info(f"🔄 Processing: {raw_table} -> {view_name}")
                columns = get_table_columns(cursor, schema_name, raw_table)
                sql_to_execute = generate_create_view_sql(schema_name, raw_table, view_name, columns)
                if sql_to_execute:
                    logging.info(f"📤 Executing SQL:\n{sql_to_execute}")
                    execute_sql_statement(conn, sql_to_execute)
    finally:
        if conn:
            conn.close()
            logging.info("🔒 Database connection closed.")

if __name__ == "__main__":
    schema = "public"
    table_view_mappings = {
        "doorloop_raw_properties": "doorloop_normalized_properties",
        "doorloop_raw_units": "doorloop_normalized_units",
        "doorloop_raw_leases": "doorloop_normalized_leases",
        "doorloop_raw_lease_payments": "doorloop_normalized_lease_payments",
        # Add the rest of your 30+ tables here...
    }

    automate_view_creation(schema, table_view_mappings)
