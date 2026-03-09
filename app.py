from datetime import datetime, UTC
from flask import Flask, jsonify
from services.ingest import ingest_posts
from services.run_log import append_run
from config import RUN_LOG_FILE
import logging

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler("logs/service.log"),
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

        result = ingest_posts()
        append_run(RUN_LOG_FILE, result)

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
                "dataset": "posts:",
                "status": "failed",
                "error": str(e),
                "timestamp_utc": datetime.now(UTC).isoformat()
            }
        ), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)