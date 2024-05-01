from minio import Minio


minio_client = Minio("localhost:9003", "min_usr", "min_pass", secure=False)
bucket_name = "result"

minio_client.fget_object(bucket_name, "tr_by_category.png", "tr_by_category.png")
minio_client.fget_object(bucket_name, "low_tr.png", "low_tr.png")
minio_client.fget_object(bucket_name, "cat_sum.png", "cat_sum.png")