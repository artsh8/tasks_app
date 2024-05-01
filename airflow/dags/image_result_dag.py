import matplotlib.pyplot as plt
import psycopg2
from minio import Minio
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def make_chart(lables, data, type, dest, xlabel=None, ylabel=None):
    fig, ax = plt.subplots(figsize=(12, 9))
    if type == "bar":
        ax.bar(lables, data)
    elif type == "pie":
        ax.pie(data, lables=lables)
    elif type == "stem":
        ax.stem(lables, data)
    elif type == "table":
        plt.table(cellText=data, colLabels=lables, loc="center").scale(1.2, 1.2)
        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
        plt.axis('off') 
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(dest)


def upload_chart():
    minio_client = Minio("10.5.0.4:9000", "min_usr", "min_pass", secure=False)
    conn = psycopg2.connect("host=10.5.0.2 port=5432 dbname=flowdb user=postgres password=postgres")
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM low_tr")
        low_tr = cur.fetchall()
        cur.execute("SELECT category, ROUND(total / 1000) FROM tr_by_category")
        tr_by_category = cur.fetchall()
        cur.execute("SELECT * FROM cat_sum")
        cat_sum = cur.fetchall()
        cat_sum_col = [desc[0] for desc in cur.description]
    conn.close()
    tr_by_category_col = [i[0] for i in tr_by_category]
    tr_by_category_data = [i[1] for i in tr_by_category]
    low_tr_date = [i[0] for i in low_tr]
    low_tr_total = [i[1] for i in low_tr]
    make_chart(tr_by_category_col, tr_by_category_data, "bar", "tr_by_category.png", ylabel="Тысяч")
    make_chart(low_tr_date, low_tr_total, "stem", "low_tr.png")
    make_chart(cat_sum_col, cat_sum, "table", "cat_sum.png")
    plt.close('all')
    minio_client.fput_object("result", "tr_by_category.png", "tr_by_category.png")
    minio_client.fput_object("result", "low_tr.png", "low_tr.png")
    minio_client.fput_object("result", "cat_sum.png", "cat_sum.png")
    os.remove("tr_by_category.png")
    os.remove("low_tr.png")
    os.remove("cat_sum.png")


with DAG(
    dag_id="upload_charts",
    start_date=datetime(2024, 4, 30),
    schedule="@daily",
    catchup=False
) as dag:
    upload_results = PythonOperator(
        task_id="upload_results",
        python_callable=upload_chart
    )
    upload_results