import snowflake.connector

from config import (
    DATASET_CONFIG,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_PRIVATE_KEY_FILE,
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
)

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        private_key_file=SNOWFLAKE_PRIVATE_KEY_FILE,
        private_key_file_pwd=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    )

def load_data_to_snowflake(table_name, column_names, rows, truncate_first="false"):
    if not rows:
        return {
            "snowflake_status": "success",
            "snowflake_table": table_name,
            "snowflake_row_count": 0
        }
    
    columns_sql = ", ".join(column_names)
    placeholders_sql = ", ".join(["%s"] * len(column_names))

    insert_sql = f"""
        INSERT INTO {table_name} (
            {columns_sql}
        )
        VALUES (
            {placeholders_sql}
        )
    """

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        if truncate_first.lower() == "true":
            cursor.execute(f"TRUNCATE TABLE {table_name}")

        cursor.executemany(insert_sql, rows)

        return {
            "snowflake_status": "success",
            "snowflake_table": table_name,
            "snowflake_row_count": len(rows)
        }
    
    finally:
        cursor.close()
        conn.close()

def build_rows_from_source_fields(data, source_fields):
    return [
        tuple(record.get(field) for field in source_fields)
        for record in data
    ]
    
def load_dataset_to_snowflake(dataset_name, data):
    if dataset_name not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset for Snowflake load: {dataset_name}")
    
    dataset_config = DATASET_CONFIG[dataset_name]

    rows = build_rows_from_source_fields(
        data=data,
        source_fields=dataset_config["source_fields"]
    )

    return load_data_to_snowflake(
        table_name=dataset_config["table_name"],
        column_names=dataset_config["target_columns"],
        rows=rows,
        truncate_first="true"
    )
