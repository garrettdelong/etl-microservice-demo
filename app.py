from datetime import datetime, UTC
from flask import Flask, jsonify
from services.ingest import ingest_posts

app = Flask(__name__)

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
        result = ingest_posts()
        return jsonify(result), 200
    except Exception as e:
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