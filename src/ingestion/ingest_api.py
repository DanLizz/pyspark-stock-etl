import os
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

API_KEY = os.getenv("ALPHAVANTAGE_KEY")
SYMBOL = os.getenv("STOCK_SYMBOL", "SHOP")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/raw/")

spark = SparkSession.builder \
    .appName("StockDataIngestion") \
    .master("local[*]").config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()



def fetch_stock_data(symbol):
    try:
        url=f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
        resp = requests.get(url)
        print(f"API response status: {resp.status_code}")
        resp.raise_for_status()
        data = resp.json().get("Time Series (Daily)", {})
        if not data:
            print(f"No records fetched for symbol {symbol}")
        else:  
            print(f"Fetched {len(data)} records for symbol {symbol}")
        records = []

        for ts, values in data.items():
            record = {
                "timestamp": ts,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"])
            }
            records.append(record)
        print(f"Processed {len(records)} records")
        return records
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

records = fetch_stock_data(SYMBOL)
print(f"fetched records")
df = spark.createDataFrame(records)
df = df.withColumn("ingestion_time", current_timestamp())
print(f"created dataframe with {df.count()} records")
df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)
print(f"written to {OUTPUT_PATH}")
# Verify the write
delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)
print("Delta table schema:")
delta_table.toDF().printSchema()
delta_table.toDF().show(5)
print(f"Delta table record count: {delta_table.toDF().count()}")
spark.stop()