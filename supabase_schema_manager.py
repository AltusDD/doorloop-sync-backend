
import requests
import logging

logger = logging.getLogger(__name__)

def ensure_table_structure(table_name: str, records: list, supabase_client):
    if not records:
        logger.warning(f"⚠️ No records provided for schema check on table '{table_name}'")
        return

    existing_columns = get_existing_columns(table_name, supabase_client)

    new_columns = infer_columns_from_records(records)
    for column_name, column_type in new_columns.items():
        if column_name not in existing_columns:
            try:
                add_column(table_name, column_name, column_type, supabase_client)
            except Exception as e:
                logger.error(f"❌ Error adding column '{column_name}' to '{table_name}': {e}")

def get_existing_columns(table_name: str, supabase_client):
    try:
        url = f"{supabase_client.rest_url}/{table_name}?limit=1"
        response = requests.get(url, headers=supabase_client.headers)
        response.raise_for_status()
        if not response.json():
            return set()
        return set(response.json()[0].keys())
    except Exception as e:
        logger.warning(f"⚠️ Could not fetch existing columns for '{table_name}': {e}")
        return set()

def infer_columns_from_records(records: list):
    type_mapping = {
        str: "text",
        int: "integer",
        float: "numeric",
        bool: "boolean",
        dict: "jsonb",
        list: "jsonb"
    }

    column_types = {}
    for record in records:
        for key, value in record.items():
            if value is None:
                continue
            py_type = type(value)
            pg_type = type_mapping.get(py_type, "text")
            column_types[key] = pg_type

    return column_types

def add_column(table: str, column: str, column_type: str, supabase_client):
    sql = f'ALTER TABLE "{table}" ADD COLUMN "{column}" {column_type};'
    logger.debug(f"📐 Adding column: {sql}")
    response = requests.post(
        f"{supabase_client.rest_url}/rpc/execute_sql",
        headers=supabase_client.headers,
        json={"sql": sql}
    )
    if not response.ok:
        logger.error(f"❌ SQL failed: {sql}")
        logger.error(f"Response: {response.text}")
        response.raise_for_status()
