from datetime import datetime, UTC
from flask import Flask, jsonify
import logging

from services.ingest import ingest_dataset
from services.run_log import append_run, read_run_history, get_latest_run
from services.run_log_snowflake import append_run_to_snowflake
from config import APP_HOST, APP_PORT, LOG_DIR, LOG_LEVEL, RUN_LOG_FILE

app = Flask(__name__)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "service.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "service": "flask-etl-service",
            "timestamp_utc": datetime.now(UTC).isoformat()
        }
    ), 200

@app.post("/ingest/posts")
def ingest_posts_route():
    try:
        logger.info("Starting ingest for posts")

        result = ingest_dataset("posts")
        append_run(RUN_LOG_FILE, result)

        try:
            append_run_to_snowflake(result)
        except Exception:
            logger.exception("Failed to append run metadata to Snowflake")

        logger.info(
            "Completed ingest for posts with %s records written to %s",
            result["record_count"],
            result["file_path"]
        )

        return jsonify(result), 200
    
    except Exception as e:
        logger.exception("Ingest failed for posts")

        return jsonify(
            {
                "dataset": "posts",
                "status": "failed",
                "error_message": str(e),
                "timestamp_utc": datetime.now(UTC).isoformat()
            }
        ), 500
    
@app.post("/ingest/users")
def ingest_users_route():
    try:
        logger.info("Starting ingest for users")

        result = ingest_dataset("users")
        append_run(RUN_LOG_FILE, result)

        try:
            append_run_to_snowflake(result)
        except Exception:
            logger.exception("Failed to append run metadata to Snowlfake")

        logger.info(
            "Completed ingest for users with %s records written to %s",
            result["record_count"],
            result["file_path"]
        )

        return jsonify(result), 200
    
    except Exception as e:
        logger.exception("Ingest failed for users")

        return jsonify(
            {
                "dataset": "users",
                "status": "failed",
                "error": str(e),
                "timestamp_utc": datetime.now(UTC).isoformat()
            }
        ), 500

@app.get("/runs")
def get_runs():
    runs = read_run_history(RUN_LOG_FILE)
    return jsonify(runs), 200

@app.get("/runs/latest")
def latest_run():
    latest = get_latest_run(RUN_LOG_FILE)

    if latest is None:
        return jsonify(
            {
                "message": "No runs found"
            }
        ), 404
    
    return jsonify(latest), 200

if __name__ == "__main__":
    app.run(host=APP_HOST, port=APP_PORT, debug=True)
