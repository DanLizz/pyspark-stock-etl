from pysaprk.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

spark = SparkSession.builder \
    .appName("StockDataTransformation") \
    .getOrCreate()

bronze = "data/raw/"
silver = "data/processed/silver/"
gold = "data/processed/gold/"

df = spark.read.format("delta").load(bronze)

clean = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna(subset=["open", "close", "volume"])\
    .dropDuplicates(["symbol", "timestamp"])

clean.write.format("delta").mode("overwrite").save(silver)
spark.stop()


