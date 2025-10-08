from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='Stock ETL DAG',
    schedule='*/5 * * * *',  # every 5 min
    start_date=datetime(2025, 10, 1),
    catchup=False
)

ingest = BashOperator(
    task_id='ingest',
    bash_command='python /opt/etl/ingest_api.py',
    dag=dag
)

transform = BashOperator(
    task_id='transform',
    bash_command='python /opt/etl/transform.py',
    dag=dag
)

features = BashOperator(
    task_id='features',
    bash_command='python /opt/etl/feature_engineering.py',
    dag=dag
)

ingest >> transform >> features
