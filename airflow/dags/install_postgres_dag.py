from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="install_postgres_drivers",
    start_date=datetime(2024, 4, 29),
    schedule="@once",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="install_postgres_drivers",
        bash_command="wget -qO /root/airflow/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
    )
    task1