def test_health_endpoint(client):
    response = client.get("/health")

    assert response.status_code == 200

    body = response.get_json()

    assert body["status"] == "ok"
    assert body["service"] == "flask-etl-service"
    assert "timestamp_utc" in body
