from pathlib import Path
from services.run_log import read_run_history, append_run, get_latest_run

def test_read_run_history_returns_empty_list_when_file_missing(tmp_path):
    log_file = tmp_path / "run_log.json"

    result = read_run_history(log_file)

    assert result == []

def test_append_run_adds_record(tmp_path):
    log_file = tmp_path / "run_log.json"

    run_record = {
        "run_id": "test-run-1",
        "dataset": "posts",
        "status": "success"
    }

    append_run(log_file, run_record)

    history = read_run_history(log_file)

    assert len(history) == 1
    assert history[0]["run_id"] == "test-run-1"
    assert history[0]["dataset"] == "posts"

def test_get_latest_run_returns_last_record(tmp_path):
    log_file = tmp_path / "run_log.json"

    append_run(log_file, {"run_id": "run-1", "dataset": "posts"})
    append_run(log_file, {"run_id": "run-2", "dataset": "users"})

    latest = get_latest_run(log_file)

    assert latest["run_id"] == "run-2"
    assert latest["dataset"] == "users"
