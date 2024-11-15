import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, initcap, coalesce
import os

# Configuração de autenticação e e-mail para falhas
def alert_email_on_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    email = "jaquerando@gmail.com"

    subject = f"Alerta de Falha - DAG: {dag_id}, Task: {task_id}"
    body = f"""
    <h3>Alerta de Falha na DAG</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task com Falha:</strong> {task_id}</p>
    <p><strong>Data de Execução:</strong> {execution_date}</p>
    <p><strong>URL do Log:</strong> <a href="{log_url}">{log_url}</a></p>
    <p>Verifique o log para detalhes da falha.</p>
    """
    send_email(to=email, subject=subject, html_content=body)

# Função de autenticação e download da chave
def download_auth_key():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('us-central1-composer-case-e66c77cc-bucket')
    blob = bucket.blob('config/case-abinbev-6d2559f17b6f.json')
    auth_key_path = '/tmp/case-abinbev-6d2559f17b6f.json'
    blob.download_to_filename(auth_key_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = auth_key_path
    logging.info(f"Arquivo de chave de autenticação baixado e configurado em: {auth_key_path}")

# Função para transformação de dados
def transform_data_to_silver():
    log_messages = ["Iniciando transformação dos dados da camada Bronze para Silver"]
    
    try:
        # Inicializa o Spark com o caminho do conector GCS
        spark = SparkSession.builder \
            .appName("BrewerySilverLayer") \
            .config("spark.jars", "/tmp/gcs-connector-hadoop3-latest.jar") \
            .getOrCreate()
        
        log_messages.append("Sessão Spark inicializada com conector GCS configurado")

        bronze_path = "gs://bucket-case-abinbev/data/bronze/breweries_raw.json"
        silver_path = "gs://bucket-case-abinbev/data/silver/breweries_transformed"
        
        # Lendo os dados da camada Bronze
        raw_df = spark.read.json(bronze_path)
        log_messages.append("Dados carregados da camada Bronze")

        # Transformações nos dados
        transformed_df = (
            raw_df
            .withColumn("id", trim(col("id")))
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("brewery_type", coalesce(col("brewery_type"), col("unknown")))
            .withColumn("address_1", initcap(trim(col("address_1"))))
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("state_province", lower(trim(col("state_province"))))
            .withColumn("postal_code", trim(col("postal_code")))
            .withColumn("country", initcap(trim(col("country"))))
            .withColumn("longitude", col("longitude").cast("double"))
            .withColumn("latitude", col("latitude").cast("double"))
            .na.drop(subset=["id", "name", "city", "state", "country"])
        )
        
        # Salvando na camada Silver
        transformed_df.write.mode("overwrite").partitionBy("country").parquet(silver_path)
        log_messages.append("Dados transformados e salvos na camada Silver")

    except Exception as e:
        log_messages.append(f"Erro na transformação: {e}")
        logging.error(f"Erro: {e}")
        raise

    save_log(log_messages)

# Função para salvar logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket('us-central1-composer-case-e66c77cc-bucket')
    log_blob = log_bucket.blob(f'logs/silver_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_blob.upload_from_string("\n".join(messages), content_type="text/plain")
    logging.info("Log salvo no bucket de logs.")

# Configuração da DAG Silver
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure
}

with DAG(
    'silver_dag',
    default_args=default_args,
    description='DAG para transformar dados da camada Bronze e salvar na camada Silver',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Task de autenticação
    download_auth_key_task = PythonOperator(
        task_id='download_auth_key',
        python_callable=download_auth_key,
    )

    # Task de transformação de dados
    transform_data = PythonOperator(
        task_id='transform_data_to_silver',
        python_callable=transform_data_to_silver,
    )

    download_auth_key_task >> transform_data
