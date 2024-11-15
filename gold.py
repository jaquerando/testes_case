from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def aggregate_data_to_gold():
    # Inicializar Spark
    spark = SparkSession.builder.appName("BreweryGoldLayer").getOrCreate()

    # Ler dados da camada Silver
    silver_path = "gs://bucket-case-abinbev/data/silver/breweries_transformed"
    silver_df = spark.read.parquet(silver_path)

    # Agregar dados por tipo de cervejaria e localização
    gold_df = (
        silver_df
        .groupBy("brewery_type", "state", "country")
        .agg(count("*").alias("brewery_count"))
    )

    # Salvar os dados agregados na camada Gold
    gold_path = "gs://bucket-case-abinbev/data/gold/breweries_aggregated"
    gold_df.write.mode("overwrite").parquet(gold_path)

    spark.stop()

with DAG(
    'gold_dag',
    default_args=default_args,
    description='DAG para agregar dados da camada Silver e salvar na camada Gold',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    aggregate_data = PythonOperator(
        task_id='aggregate_data_to_gold',
        python_callable=aggregate_data_to_gold,
    )

    aggregate_data
