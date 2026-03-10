import requests
import json
from datetime import datetime, UTC

from config import (
    JSONPLACEHOLDER_POSTS_URL, 
    OUTPUT_DIR,
    ENABLE_SNOWFLAKE_LOAD
)
from services.snowflake_loader import load_posts_to_snowflake

def ingest_posts():
    started_at = datetime.now(UTC).isoformat()

    response = requests.get(JSONPLACEHOLDER_POSTS_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_file = OUTPUT_DIR / f"posts_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    result = {
        "dataset": "posts",
        "source_url": JSONPLACEHOLDER_POSTS_URL,
        "status": "success",
        "record_count": len(data),
        "file_path": str(output_file),
        "started_at": started_at
    }

    if ENABLE_SNOWFLAKE_LOAD == "true":
        snowflake_result = load_posts_to_snowflake(data)
        result.update(snowflake_result)
    else: 
        result.update(
            {
                "snowflake_status": "skipped",
                "snowflake_table": "",
                "snowflake_row_count": 0
            }
        )

    result["completed_at"] = datetime.now(UTC).isoformat()

    return result
