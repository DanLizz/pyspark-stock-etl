import os
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

API+KEY = os.getenv("ALPHAVANTAGE_KEY")
SYMBOL = os.getenv("STOCK_SYMBOL", "PNG")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "data/bronze/stock")

spark = SparkSession.builder \
    .appName("StockDataIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def fetch_stock_data(symbol):
    url=f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json().get("Time Series (1min)", {})
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
        records.update({k.replace(' ', '').lower(): v for k, v in record.items()})
        records.append(record)
    return records

records = fetch_stock_data(SYMBOL)
df = spark.createDataFrame(records)
df = df.withColumn("ingestion_time", current_timestamp())

df.write.format("delta").mode("overwrite").save(OUTPUT_PATH)

spark.stop()