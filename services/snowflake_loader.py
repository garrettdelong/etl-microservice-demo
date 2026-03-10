import snowflake.connector

from config import (
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
    
def load_posts_to_snowflake(data):
    table_name = "posts"

    column_names = [
        "post_id",
        "user_id",
        "title",
        "body"
    ]

    rows = [
        (
            post["id"],
            post["userId"],
            post["title"],
            post["body"]
        )
        for post in data
    ]

    return load_data_to_snowflake(
        table_name=table_name,
        column_names=column_names,
        rows=rows,
        truncate_first="true"
    )
