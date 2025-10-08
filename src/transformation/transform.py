from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("StockDataTransformation") \
    .master("local[*]").config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

bronze = "data/raw/"
silver = "data/processed/silver/"
gold = "data/processed/gold/"

df = spark.read.format("delta").load(bronze)
print(f"Read {df.count()} records from bronze")
print("Schema:")
df.printSchema()
clean = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .dropna(subset=["open", "close", "volume"])\
    .dropDuplicates(["timestamp"])
print(f"Transformed to {clean.count()} clean records")
clean.write.format("delta").mode("overwrite").save(silver)
print(f"Written clean data to silver at {silver}")
# Verify the write
delta_table = DeltaTable.forPath(spark, silver)
print("Delta table schema:")
delta_table.toDF().printSchema()
delta_table.toDF().show(5)
print(f"Delta table record count: {delta_table.toDF().count()}")
spark.stop()


