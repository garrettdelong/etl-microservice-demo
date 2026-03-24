# Flask ETL Microservice Demo

A small Flask-based ETL microservice that ingests public API data, writes flat-file outputs, optionally loads the data into Snowflake, and can be orchestrated with Airflow.

This project was built as a focused hands-on exercise to practice Flask in a data engineering context. The goal was to keep the scope small while still showing realistic patterns that come up in production-style workflows, including API ingestion, environment-based configuration, Dockerized services, Snowflake loading, and Airflow orchestration.

## Project Overview

This project is a small Flask-based ETL microservice built to simulate a service-oriented data pipeline. The app exposes API endpoints that trigger ingestion workflows for public API datasets, writes raw outputs to local JSON files, runs data quality checks, and conditionally loads trusted data into Snowflake.

The main workflow is:

1. receive a request through a Flask endpoint
2. fetch dataset records from a JSONPlaceholder API endpoint
3.  write the raw response to a local output file
4.  run pre-load data quality checks
5.  load the dataset into Snowflake if critical checks pass
6.  run post-load validation on the Snowflake load result
7.  return structured run metadata in the API response
8.  record run history locally and in a Snowflake run log table

Airflow is used as a lightweight orchestration layer that calls the Flask ETL endpoints rather than duplicating ETL logic inside the DAG. GitHub Actions and pytest were added to give the project a basic CI and testing workflow.

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

## API Endpoints

### `GET /health`
Returns a simple health check response for the service. This endpoint is used to confirm that the Flask application is running and able to accept requests.

### `POST /ingest/posts`
Triggers an ETL run for the JSONPlaceholder posts dataset. The service fetches post records from the public API, writes the raw response to a local JSON output file, records run metadata, and optionally loads the data into the Snowflake `posts` table.

### `POST /ingest/users`
Triggers an ETL run for the JSONPlaceholder users dataset. The service fetches user records from the public API, writes the raw response to a local JSON output file, records run metadata, and optionally loads the data into the Snowflake `users` table.

### `GET /runs`
Returns the full run history stored by the service. This endpoint can be used to review prior ETL runs and inspect metadata such as dataset name, status, timestamps, output file path, and Snowflake load details.

### `GET /runs/latest`
Returns the most recent recorded ETL run. This is useful for quickly checking the latest execution result without reading the full run history.

---

## Data Quality Checks

The service now includes both pre-load and post-load data quality checks.

### Pre-Load Checks

Pre-load checks run before data is loaded into Snowflake. If a critical pre-load check fails, the service skips the Snowflake load and returns a failed response.

Current pre-load checks include:

- record count greater than zero
- required fields present
- primary key not null
- primary key unique

### Post-Load Checks

Post-load checks run after a successful Snowflake load.

Current post-load checks include:

- Snowflake row count matches extracted record count

### Quality Result Metadata

Each run now records quality metadata, including:

- `quality_status`
- `quality_checks_passed`
- `quality_checks_failed`
- `quality_check_results`

Example shape:

```json
{
  "quality_status": "failed",
  "quality_checks_passed": 3,
  "quality_checks_failed": 1,
  "quality_check_results": [
    {
      "check_name": "record_count_gt_zero",
      "status": "passed",
      "details": "record_count was 10"
    },
    {
      "check_name": "required_fields_present",
      "status": "failed",
      "details": "missing fields: ['test']"
    }
  ]
}
```

## Testing

The project includes a basic pytest suite to validate core service behavior without relying on live external systems.

### What is currently tested

The current tests focus on the highest-value parts of the app first:

- Flask health endpoint
- local run log functions
- data quality logic

This keeps the test suite fast and practical while still covering the main control flow and validation behavior of the service.

### Test structure

```text
tests/
  conftest.py
  test_health.py
  test_run_log.py
  test_data_quality.py
```