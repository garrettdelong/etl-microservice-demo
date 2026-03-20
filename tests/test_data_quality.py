from services.data_quality import run_pre_load_data_quality_checks

def test_pre_load_quality_checks_pass_for_valid_posts():
    data = [
        {
            "id": 1,
            "userId": 1,
            "title": "title 1",
            "body": "body 1"
        },
        {
            "id": 2,
            "userId": 2,
            "title": "title 2",
            "body": "body 2"
        }
    ]

    result = run_pre_load_data_quality_checks("posts", data)

    assert result["quality_status"] == "passed"
    assert result["quality_checks_failed"] == 0

def test_pre_load_quality_checks_fail_when_required_field_missing():
    data = [
        {
            "id": 1,
            "userId": 1,
            "title": "title 1"
        }
    ]

    result = run_pre_load_data_quality_checks("posts", data)

    assert result["quality_status"] == "failed"
    assert result["quality_checks_failed"] >= 1

def test_pre_load_quality_checks_fail_when_primary_key_is_null():
    data = [
        {
            "id": None,
            "userId": 1,
            "title": "title 1",
            "body": "body 1"
        }
    ]

    result = run_pre_load_data_quality_checks("posts", data)

    assert result["quality_status"] == "failed"

    failed_checks = [
        check for check in result["quality_check_results"]
        if check["status"] == "failed"
    ]

    check_names = [check["check_name"] for check in failed_checks]

    assert "primary_key_not_null" in check_names
