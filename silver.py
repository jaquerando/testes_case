import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd
import json

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

# Função para transformar dados usando Pandas
def transform_data_to_silver():
    log_messages = ["Iniciando transformação dos dados da camada Bronze para Silver"]

    try:
        bronze_path = "gs://bucket-case-abinbev/data/bronze/breweries_raw.json"
        silver_path = "gs://bucket-case-abinbev/data/silver/breweries_transformed/"

        # Conectar ao GCS e baixar os dados Bronze
        client = storage.Client()
        bucket = client.get_bucket("bucket-case-abinbev")
        blob = bucket.blob("data/bronze/breweries_raw.json")
        raw_data = json.loads(blob.download_as_text())
        log_messages.append("Dados carregados da camada Bronze")

        # Processar os dados com Pandas
        df = pd.json_normalize(raw_data)
        log_messages.append("Dados processados usando Pandas")

        # Transformações
        df["id"] = df["id"].str.strip()
        df["name"] = df["name"].str.title().str.strip()
        df["brewery_type"] = df["brewery_type"].fillna("unknown")
        df["city"] = df["city"].str.title().str.strip()
        df["state_province"] = df["state_province"].str.lower().str.strip()
        df["postal_code"] = df["postal_code"].str.strip()
        df["country"] = df["country"].str.title().str.strip()
        df = df.dropna(subset=["id", "name", "city", "state_province", "country"])

        # Salvar o resultado no GCS
        transformed_data = df.to_json(orient="records", lines=True)
        silver_blob = bucket.blob("data/silver/breweries_transformed/breweries_transformed.json")
        silver_blob.upload_from_string(transformed_data, content_type="application/json")
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
