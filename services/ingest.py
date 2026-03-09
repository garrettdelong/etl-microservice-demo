import requests
import json
from datetime import datetime, UTC

from config import JSONPLACEHOLDER_POSTS_URL, OUTPUT_DIR

def ingest_posts():
    started_at = datetime.now(UTC).isoformat()

    response = requests.get(JSONPLACEHOLDER_POSTS_URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_file = OUTPUT_DIR / f"posts_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    completed_at = datetime.now(UTC).isoformat()

    return {
        "dataset": "posts",
        "source_url": JSONPLACEHOLDER_POSTS_URL,
        "status": "success",
        "record_count": len(data),
        "file_path": str(output_file),
        "started_at": started_at,
        "completed_at": completed_at
    }
