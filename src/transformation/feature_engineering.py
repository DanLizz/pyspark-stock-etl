from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("stock_features") \
    .master("local[*]").config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

silver = "data/processed/silver/"
gold = "data/processed/gold/"

df = spark.read.format("delta").load(silver)
print(f"Read {df.count()} records from silver")
print("Schema:")
df.printSchema()
# Calculate 10-day moving average of closing price
window_spec = Window.orderBy("timestamp").rowsBetween(-10, 0)

features = df.withColumn("moving_avg_close", avg(col("close")).over(window_spec))
print(f"Calculated features for {features.count()} records")
features.write.format("delta").mode("overwrite").save(gold)
print(f"Written features to gold at {gold}")
# Verify the write
delta_table = DeltaTable.forPath(spark, gold)
print("Delta table schema:")
delta_table.toDF().printSchema()
delta_table.toDF().show(5)
print(f"Delta table record count: {delta_table.toDF().count()}")
spark.stop()