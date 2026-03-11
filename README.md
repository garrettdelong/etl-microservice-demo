# Flask ETL Microservice Demo

A small Flask-based ETL microservice that ingests public API data, writes flat-file outputs, optionally loads the data into Snowflake, and can be orchestrated with Airflow.

This project was built as a focused hands-on exercise to practice Flask in a data engineering context. The goal was to keep the scope small while still showing realistic patterns that come up in production-style workflows, including API ingestion, environment-based configuration, Dockerized services, Snowflake loading, and Airflow orchestration.

## Project Overview

The service exposes a small set of API endpoints that trigger ETL logic behind a Flask app.

The main workflow is:

1. Receive a request through a Flask endpoint
2. Call the JSONPlaceholder posts API
3. Write the response to a local output file
4. Optionally load the results into Snowflake
5. Return run metadata in the API response
6. Track run history and basic service logs

Airflow is used as a lightweight orchestration layer that triggers the Flask ETL endpoint.

## Features

- Flask API service
- Public API ingestion using JSONPlaceholder
- Flat-file output to local JSON files
- Optional Snowflake loading
- Environment-based configuration
- Snowflake key-pair authentication
- Basic service logging
- Run history tracking
- Docker support
- Airflow orchestration

## Architecture

This project separates responsibilities across three layers:

### Flask
Flask acts as the service layer. It exposes the ETL workflow through API endpoints and owns the ingestion logic.

### Snowflake
Snowflake acts as the warehouse target. When enabled, ingested data is loaded into a dedicated Snowflake database and schema.

### Airflow
Airflow acts as the orchestration layer. It triggers the Flask ingest endpoint rather than duplicating the ETL logic inside DAG tasks.

## Endpoints

### `GET /health`

Returns a simple health response for the service.

Example response:

```json
{
  "status": "ok",
  "service": "flask-etl-service",
  "timestamp_utc": "2026-03-11T00:00:00+00:00"
}
