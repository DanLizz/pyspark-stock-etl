from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("stock_features") \
    .getOrCreate()

silver = "data/processed/silver/"
gold = "data/processed/gold/"

df = spark.read.format("delta").load(silver)
window_spec = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-10, 0)

features = df.withColumn("moving_avg_close", avg(col("close")).over(window_spec))

features.write.format("delta").mode("overwrite").save(gold)

spark.stop()