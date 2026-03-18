from config import DATASET_CONFIG

def run_pre_load_data_quality_checks(dataset_name, data):
    if dataset_name not in DATASET_CONFIG:
        raise ValueError(f"Unspopported dataset for quality checks: {dataset_name}")
    
    dataset_config = DATASET_CONFIG[dataset_name]
    source_fields = dataset_config["source_fields"]
    primary_key_field = dataset_config["primary_key_field"]

    check_results = []

    record_count = len(data)
    if record_count > 0:
        check_results.append(
            {
                "check_name": "record_count_gt_zero",
                "status": "passed",
                "details": f"record_count was {record_count}"
            }
        )
    else:
        check_results.append(
            {
                "check_name": "record_count_gt_zero",
                "status": "failed",
                "details": "recourd_count was 0"
            }
        )

    actual_fields = sorted({key for row in data for key in row.keys()}) if data else []
    missing_fields = [field for field in source_fields if field not in actual_fields]

    if not missing_fields:
        check_results.append(
            {
                "check_name": "required_fields_present",
                "status": "passed",
                "details": "all required fields were present"
            }
        )
    else:
        check_results.append(
            {
                "check_name": "required_fields_present",
                "status": "failed",
                "details": f"missing fields: {missing_fields}"
            }
        )
    
    primary_key_values = [row.get(primary_key_field) for row in data]
    null_primary_key_count = sum(1 for value in primary_key_values if value is None)
    non_null_primary_key_values = [value for value in primary_key_values if value is not None]
    duplicate_count = len(non_null_primary_key_values) - len(set(non_null_primary_key_values))

    if duplicate_count == 0 and len(non_null_primary_key_values) == len(data):
        check_results.append(
            {
                "check_name": "primary_key_unique",
                "status": "passed",
                "details": f"no duplicate {primary_key_field} values found"
            }
        )
    else:
        check_results.append(
            {
                "check_name": "primary_key_unique",
                "status": "failed",
                "details": f"found {duplicate_count} duplicated {primary_key_field} values"
            }
        )

    if null_primary_key_count == 0:
        check_results.append(
            {
                "check_name": "primary_key_not_null",
                "status": "passed",
                "details": f"no null {primary_key_field} values found"
            }
        )
    else:
        check_results.append(
            {
                "check_name": "primary_key_not_null",
                "status": "failed",
                "details": f"found {null_primary_key_count} null {primary_key_field} values"
            }
        )
     
    passed_count = sum(1 for check in check_results if check["status"] == "passed")
    failed_count = sum(1 for check in check_results if check["status"] == "failed")

    quality_status = "passed" if failed_count == 0 else "failed"

    return {
        "quality_status": quality_status,
        "quality_checks_passed": passed_count,
        "quality_checks_failed": failed_count,
        "quality_check_results": check_results
    }

def run_post_load_data_quality_checks(record_count, snowflake_row_count, existing_results):
    check_results = list(existing_results.get("quality_check_results", []))

    if snowflake_row_count == record_count:
        check_results.append(
            {
                "check_name": "snowflake_row_count_matches_record_count",
                "status": "passed",
                "details": f"snowflake_row_count matched record_count at {record_count}"
            }
        )
    else:
        check_results.append(
            {
                "check_name": "snowflake_row_count_matches_record_count",
                "status": "failed",
                "details": f"snowflake_row_count was {snowflake_row_count} but record_count was {record_count}"
            }
        )

    passed_count = sum(1 for check in check_results if check["status"] == "passed")
    failed_count = sum(1 for check in check_results if check["status"] == "failed")
    quality_status = "passed" if failed_count == 0 else "failed"

    return {
        "quality_status": quality_status,
        "quality_checks_passed": passed_count,
        "quality_checks_failed": failed_count,
        "quality_check_results": check_results
    }
