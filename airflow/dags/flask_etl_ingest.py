from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id="flask_etl_ingest",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["flask", "etl", "snowflake"]
) as dag:
    
    ingest_posts = HttpOperator(
        task_id="ingest_posts",
        http_conn_id="flask_etl_service",
        endpoint="ingest/posts",
        method="POST",
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes=1)
    )

    ingest_users = HttpOperator(
        task_id="ingest_users",
        http_conn_id="flask_etl_service",
        endpoint="ingest/users",
        method="POST",
        log_response=True,
        retries=2,
        retry_delay=timedelta(minutes=1)
    )

    ingest_posts >> ingest_users
