from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("tasks_postgres") \
    .config("spark.jars", "/opt/bitnami/spark/postgres_jar/postgresql-42.7.3.jar") \
    .getOrCreate()

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://10.5.0.2:5432/flowdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.transaction") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()


tr_by_category = df.groupBy("category").agg(F.sum("amount").alias("total"))

byCategory = Window.partitionBy("category")
byMerchant = Window.partitionBy("category", "merchant_name")
byShare = Window.partitionBy("category").orderBy(F.desc("merchant_share"))
cat_sum = (
    df.withColumn("category_sum", F.sum(df["amount"]).over(byCategory))
    .withColumn("merchant_total", F.sum(df["amount"]).over(byMerchant))
    .withColumn("merchant_share", F.round(F.col("merchant_total") / F.col("category_sum") * 100, 6))
    .withColumn("merchant_rank", F.dense_rank().over(byShare)) 
    .filter(F.col("merchant_rank") == 1)
    .select("category", "merchant_name", "category_sum", "merchant_total", "merchant_share", "merchant_rank")
    .distinct()
          )

low_tr = df.groupBy("date").agg(F.sum("amount").alias("total")).orderBy("total").limit(5)


low_tr.write.format("jdbc") \
    .option("url", "jdbc:postgresql://10.5.0.2:5432/flowdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.low_tr") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite").save()

cat_sum.write.format("jdbc") \
    .option("url", "jdbc:postgresql://10.5.0.2:5432/flowdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.cat_sum") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite").save()

tr_by_category.write.format("jdbc") \
    .option("url", "jdbc:postgresql://10.5.0.2:5432/flowdb") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.tr_by_category") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite").save()

spark.stop()