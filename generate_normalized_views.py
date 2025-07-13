import os
import logging
import psycopg2
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_table_names():
    return [
        "doorloop_raw_properties",
        "doorloop_raw_units",
        "doorloop_raw_tenants",
        "doorloop_raw_owners",
        "doorloop_raw_leases",
        "doorloop_raw_lease_payments",
        "doorloop_raw_lease_charges",
        "doorloop_raw_lease_credits",
        "doorloop_raw_vendors",
        "doorloop_raw_tasks",
        "doorloop_raw_files",
        "doorloop_raw_notes",
        "doorloop_raw_communications",
        "doorloop_raw_applications",
        "doorloop_raw_inspections",
        "doorloop_raw_insurance_policies",
        "doorloop_raw_recurring_charges",
        "doorloop_raw_recurring_credits",
        "doorloop_raw_accounts",
        "doorloop_raw_users",
        "doorloop_raw_portfolios",
        "doorloop_raw_reports",
        "doorloop_raw_activity_logs"
    ]

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def fetch_sample_payload(cursor, table_name):
    cursor.execute(f"SELECT payload_json FROM {table_name} WHERE payload_json IS NOT NULL LIMIT 1")
    row = cursor.fetchone()
    if not row:
        raise Exception(f"No payload found in {table_name}")
    return row[0]

def generate_view_sql(table_name, sample_payload):
    columns = []
    for key in sample_payload.keys():
        if key.lower() in ("id", "doorloop_id", "payload_json", "created_at"):
            continue
        col = f"payload_json->>'{key}' AS {key}"
        columns.append(col)
    column_sql = ",
  ".join(columns)
    view_name = table_name.replace("doorloop_raw_", "")
    sql = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT
      id,
      doorloop_id,
      created_at,
      {column_sql}
    FROM {table_name};
    """
    return sql

def main():
    logger.info("Starting normalized view generation...")
    conn = connect_db()
    cur = conn.cursor()

    for table in get_table_names():
        logger.info(f"🔧 Processing view for: {table}")
        try:
            sample = fetch_sample_payload(cur, table)
            sql = generate_view_sql(table, sample)
            logger.info(f"📤 Executing SQL: {sql.strip().splitlines()[0]}")
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed for {table}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    logger.info("✅ View generation completed.")

if __name__ == "__main__":
    main()
