
import psycopg2
from psycopg2 import sql
import os
import logging

# Configure logging
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
        logging.info("Connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

def get_table_columns(cursor, schema, table):
    cursor.execute(
        sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """),
        [schema, table]
    )
    return [row[0] for row in cursor.fetchall()]

def generate_create_view_sql(schema, raw_table, view_name, columns):
    quoted_columns = [sql.Identifier(col) for col in columns]
    select_clause = sql.SQL(', ').join(quoted_columns)
    return sql.SQL("""
        CREATE OR REPLACE VIEW {view} AS
        SELECT {fields}
        FROM {table};
    """).format(
        view=sql.Identifier(schema, view_name),
        fields=select_clause,
        table=sql.Identifier(schema, raw_table)
    )

def execute_sql(conn, statement):
    try:
        with conn.cursor() as cursor:
            cursor.execute(statement)
        conn.commit()
        logging.info("Executed view creation SQL.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed SQL execution: {e}")

def automate_view_creation(schema, mappings):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for raw_table, view_name in mappings.items():
                logging.info(f"Processing {raw_table} -> {view_name}")
                columns = get_table_columns(cursor, schema, raw_table)
                if columns:
                    view_sql = generate_create_view_sql(schema, raw_table, view_name, columns)
                    execute_sql(conn, view_sql)
                else:
                    logging.warning(f"No columns found for {raw_table}")
    finally:
        conn.close()

if __name__ == "__main__":
    schema = "public"
    mappings = {
        "doorloop_raw_properties": "doorloop_normalized_properties",
        "doorloop_raw_units": "doorloop_normalized_units",
        # add more raw->normalized mappings here
    }
    automate_view_creation(schema, mappings)
    