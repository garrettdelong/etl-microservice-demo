import json
from pathlib import Path

def read_run_history(log_file: Path):
    if not log_file.exists():
        return []
    
    with open(log_file, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return []
        return json.loads(content)
    
def append_run(log_file: Path, run_record: dict):
    history = read_run_history(log_file)
    history.append(run_record)

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)
        
def get_latest_run(log_file: Path):
    history = read_run_history(log_file)
    if not history:
        return None
    return history[-1]
