from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import psycopg2
import csv

def minio_data_import():
    minio_client = Minio("10.5.0.4:9000", "min_usr", "min_pass", secure=False)
    conn = psycopg2.connect("host=10.5.0.2 port=5432 dbname=flowdb user=postgres password=postgres")
    minio_client.fget_object("source", "credit_card_transaction_flow.csv", "credit_card_transaction_flow.csv")    
    with open("credit_card_transaction_flow.csv", 'r') as file:
        reader = csv.reader(file)
        next(reader)
        data = [tuple(row) for row in reader]
    os.remove("credit_card_transaction_flow.csv")
    with conn.cursor() as cur:
        cur.execute("TRUNCATE tmp_transaction;")
        sql = """
        INSERT INTO tmp_transaction(
        id,
        name,
        surename,
        gender,
        birth_date,
        amount,
        date,
        merchant_name,
        category)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(sql, data)
    conn.commit()

with DAG(
    dag_id="setup_schema",
    start_date=datetime(2024, 4, 18),
    schedule="@once",
    catchup=False,
) as dag:
    create_transaction_table = PostgresOperator(
        task_id="create_transaction_table",
        sql="""
            CREATE TABLE IF NOT EXISTS transaction (
            id int4,
            name varchar(100),
            surename varchar(100),
            gender varchar(1),
            birth_date date,
            amount numeric(9,2),
            date date,
            merchant_name varchar(100),
            category varchar(50),
            PRIMARY KEY (id));

            CREATE TABLE IF NOT EXISTS tmp_transaction(
            id varchar(100),
            name varchar(100),
            surename varchar(100),
            gender varchar(100),
            birth_date varchar(100),
            amount varchar(100),
            date varchar(100),
            merchant_name varchar(100),
            category varchar(100));
            """
    )
    data_import = PythonOperator(
        task_id="data_import",
       python_callable=minio_data_import
    )
    migrate_data = PostgresOperator(
         task_id="migrate_data",
         sql="""
            INSERT INTO transaction(
                id,
                name,
                surename,
                gender,
                birth_date,
                amount,
                date,
                merchant_name,
                category
                )
            SELECT 
                id::int,
                name,
                surename,
                gender,
                to_date(birth_date, 'DD-MM-YYYY'),
                amount::numeric,
                to_date(date, 'DD-MM-YYYY'),
                merchant_name,
                category
            FROM tmp_transaction;

            TRUNCATE tmp_transaction;
            """
    )
    create_result_tables = PostgresOperator(
        task_id="create_result_tables",
        sql="""
            CREATE TABLE IF NOT EXISTS low_tr(
            date date,
            total numeric(9, 2));

            CREATE TABLE IF NOT EXISTS cat_sum(
            category varchar(100),
            merchant_name varchar(100),
            category_sum numeric(12, 2),
            merchant_total numeric(9, 2),
            merchant_share numeric(7, 6),
            merchant_rank int2);

            CREATE TABLE IF NOT EXISTS tr_by_category(
            category varchar(100),
            total numeric(9, 2));
            """
    )
    create_transaction_table >> data_import >> migrate_data >> create_result_tables