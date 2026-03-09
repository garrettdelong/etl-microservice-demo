from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
RUN_LOG_FILE = LOG_DIR / "runs_history.json"

OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

JSONPLACEHOLDER_POSTS_URL = os.getenv(
    "JSONPLACEHOLDER_POSTS_URL",
    "https://jsonplaceholder.typicode.com/posts"
)
