import streamlit as st
import pandas as pd
import altair as alt
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

st.set_page_config(page_title="Real-time Stock Dashboard", layout="wide")

spark = SparkSession.builder.appName("stock_dashboard") \
    .master("local[*]").config("spark.driver.host", "127.0.0.1") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

gold_path = "data/processed/gold/"
df = spark.read.format("delta").load(gold_path)
print(f"Read {df.count()} records from gold")
pdf = df.toPandas()
print(f"Converted to pandas with {len(pdf)} records")
st.title(f"📈 Stock Dashboard:")
chart = alt.Chart(pdf).mark_line().encode(
    x='event_time:T',
    y='close:Q'
)
st.altair_chart(chart, use_container_width=True)

# Rolling avg chart
rolling_chart = alt.Chart(pdf).mark_line(color='red').encode(
    x='event_time:T',
    y='rolling_avg_10:Q'
)
st.altair_chart(rolling_chart, use_container_width=True)
print("Dashboard rendered")