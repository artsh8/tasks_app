from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="spark_dag",
    default_args=default_args,
    start_date=datetime(2024, 4, 14),
    schedule_interval="@daily"
    ) as dag:

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        application="/root/airflow/sp_script.py",
        conn_id="spark_default",
        conf={"spark.jars": "/root/airflow/postgresql-42.7.3.jar"},
        verbose=True
    )

    submit_job
