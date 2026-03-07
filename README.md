# Flask ETL Microservice

A small Flask-based ETL microservice that exposes simple API endpoints to ingest public data, write output to local files, and track run metadata.

This project was built as a lightweight hands-on exercise to practice Flask in an ETL-style workflow. The goal was to keep the scope small while still reflecting patterns that show up in real data engineering work, including API ingestion, file-based outputs, logging, Docker support, and operational endpoints.

## Project Overview

The service provides a few simple endpoints:

- `GET /health`
- `POST /ingest/posts`
- `GET /runs`
- `GET /runs/latest`

The ingest endpoint calls the JSONPlaceholder API, retrieves a dataset, writes the results to a local file, and records metadata about the run.

## Features

- Flask API service
- Public API ingestion using JSONPlaceholder
- Output written to local JSON or CSV files
- Run metadata logging
- Health check endpoint
- Basic structured logging
- Docker support

## Tech Stack

- Python
- Flask
- Requests
- Pandas
- Docker

## Project Structure

```text
flask_etl_service/
  app.py
  config.py
  requirements.txt
  Dockerfile
  README.md
  services/
    __init__.py
    ingest.py
    run_log.py
  output/
  logs/
