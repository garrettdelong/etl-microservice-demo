import requests
import json
from datetime import datetime, UTC
import uuid

from config import (
    DATASET_CONFIG,
    OUTPUT_DIR,
    ENABLE_SNOWFLAKE_LOAD
)
from services.snowflake_loader import load_dataset_to_snowflake
from services.data_quality import run_pre_load_data_quality_checks, run_post_load_data_quality_checks

def write_output_file(output_prefix, data):
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_file = OUTPUT_DIR / f"{output_prefix}_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return output_file

def ingest_dataset(dataset_name):
    if dataset_name not in DATASET_CONFIG:
        raise ValueError(f"Unknown dataset: {dataset_name}")
    
    dataset_config = DATASET_CONFIG[dataset_name]
    source_url = dataset_config["source_url"]
    output_prefix = dataset_config["output_prefix"]

    started_at = datetime.now(UTC).isoformat()

    response = requests.get(source_url, timeout=30)
    response.raise_for_status()

    data = response.json()
    output_file = write_output_file(output_prefix, data)

    result = {
        "run_id": str(uuid.uuid4()),
        "dataset": dataset_name,
        "source_url": source_url,
        "status": "success",
        "record_count": len(data),
        "file_path": str(output_file),
        "started_at": started_at
    }

    pre_quality_result = run_pre_load_data_quality_checks(
        dataset_name=dataset_name,
        data=data
    )

    result.update(pre_quality_result)

    if result["quality_status"] == "failed":
        result.update(
            {
                "status": "failed",
                "snowflake_status": "skipped",
                "snowlfake_table": dataset_config["table_name"],
                "snowflake_row_count": 0,
                "error_message": "Pre-load data qualit checks failed"
            }
        )
        result["completed_at"] = datetime.now(UTC).isoformat()
        return result


    if ENABLE_SNOWFLAKE_LOAD == "true":
        snowflake_result = load_dataset_to_snowflake(dataset_name, data)
        result.update(snowflake_result)

        post_quality_result = run_post_load_data_quality_checks(
            record_count=result["record_count"],
            snowflake_row_count=result.get("snowflake_row_count", 0),
            existing_results=result
        )
        result.update(post_quality_result)
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
