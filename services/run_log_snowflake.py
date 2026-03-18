import json
import logging
from services.snowflake_loader import get_snowflake_connection

logger = logging.getLogger(__name__)

def append_run_to_snowflake(run_record: dict):

    insert_sql = """
        INSERT INTO etl_run_log (
            run_id,
            dataset,
            status,
            source_url,
            file_path,
            record_count,
            snowflake_status,
            snowflake_table,
            snowflake_row_count,
            quality_status,
            quality_checks_passed,
            quality_checks_failed,
            quality_check_results,
            error_message,
            started_at,
            completed_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s
        )
        """
    row = (
        run_record.get("run_id", ""),
        run_record.get("dataset", ""),
        run_record.get("status", ""),
        run_record.get("source_url", ""),
        run_record.get("file_path", ""),
        run_record.get("record_count", 0),
        run_record.get("snowflake_status", ""),
        run_record.get("snowflake_table", ""),
        run_record.get("snowflake_row_count", 0),
        run_record.get("quality_status", ""),
        run_record.get("quality_checks_passed", 0),
        run_record.get("quality_checks_failed", 0),
        json.dumps(run_record.get("quality_check_results", [])),
        run_record.get("error_message", ""),
        run_record.get("started_at"),
        run_record.get("completed_at")            
    )

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(insert_sql, row)
    except Exception:
        logger.exception("Failed to append run log to Snowflake")
        raise
    finally:
        cursor.close()
        conn.close()
