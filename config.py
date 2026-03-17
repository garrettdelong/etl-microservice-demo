from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = os.getenv("ENV_FILE", ".env")
load_dotenv(BASE_DIR / ENV_FILE)

def get_env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()

def get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)

OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
RUN_LOG_FILE = LOG_DIR / "runs_history.json"

OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

APP_HOST = get_env("APP_HOST", "0.0.0.0")
APP_PORT = get_env_int("APP_PORT", 5000)
LOG_LEVEL = get_env("LOG_LEVEL", "INFO")

JSONPLACEHOLDER_POSTS_URL = os.getenv(
    "JSONPLACEHOLDER_POSTS_URL",
    "https://jsonplaceholder.typicode.com/posts"
)

JSONPLACEHOLDER_USERS_URL = os.getenv(
    "JSONPLACEHOLDER_USERS_URL",
    "https://jsonplaceholder.typicode.com/users"
)

DATASET_CONFIG = {
    "posts": {
        "source_url": JSONPLACEHOLDER_POSTS_URL,
        "output_prefix": "posts",
        "table_name": "posts",
        "source_fields": ["id", "userId", "title", "body"],
        "target_columns": ["post_id", "user_id", "title", "body"]

    },
    "users": {
        "source_url": JSONPLACEHOLDER_USERS_URL,
        "output_prefix": "users",
        "table_name": "users",
        "source_fields": ["id", "name", "username", "email", "phone", "website"],
        "target_columns": ["user_id", "name", "username", "email", "phone", "website"]
    }
}

DEFAULT_OUTPUT_FORMAT = get_env("DEFAULT_OUTPUT_FORMAT", "json")

ENABLE_SNOWFLAKE_LOAD = get_env("ENABLE_SNOWFLAKE_LOAD", "false").lower()

SNOWFLAKE_ACCOUNT = get_env("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = get_env("SNOWFLAKE_USER")
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = get_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
SNOWFLAKE_WAREHOUSE = get_env("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = get_env("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = get_env("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = get_env("SNOWFLAKE_ROLE")
SNOWFLAKE_PRIVATE_KEY_FILE = get_env("SNOWFLAKE_PRIVATE_KEY_FILE")

AIRFLOW_BASE_URL = get_env("AIRFLOW_BASE_URL")
AIRFLOW_DAG_ID = get_env("AIRFLOW_DAG_ID")
