import psycopg2
from minio import Minio


minio_client = Minio("localhost:9003", "min_usr", "min_pass", secure=False)

bucket_csv = "source"
local_csv = "credit_card_transaction_flow.csv"
dest_csv = "credit_card_transaction_flow.csv"
minio_client.fput_object(bucket_csv, dest_csv, local_csv)


conn = psycopg2.connect("host=localhost port=5433 dbname=postgres user=postgres password=postgres")
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

with conn.cursor() as cur:
    cur.execute("CREATE DATABASE flowdb;")

conn.close()
