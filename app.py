from datetime import datetime, UTC
from flask import Flask, jsonify

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)