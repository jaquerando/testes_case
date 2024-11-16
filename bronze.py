import logging
import json
import requests
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage

# Configuração para envio de e-mail em caso de falha
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

# Variáveis de configuração e log
url = "https://api.openbrewerydb.org/breweries"
bucket_name = 'bucket-case-abinbev'
blob_name = 'data/bronze/breweries_raw.json'
log_bucket_name = 'us-central1-composer-case-e66c77cc-bucket'
log_messages = []

def log_message(message):
    log_messages.append(f"\n{message}\n{'-'*40}")

def get_last_hash_from_log():
    client = storage.Client()
    log_bucket = client.get_bucket(log_bucket_name)
    blobs = list(log_bucket.list_blobs(prefix='logs/'))
    blobs.sort(key=lambda x: x.name, reverse=True)

    if blobs:
        latest_log_blob = blobs[0]
        log_content = latest_log_blob.download_as_text()
        for line in log_content.splitlines():
            if "Hash dos dados atuais baixados:" in line:
                last_hash = line.split(":")[-1].strip()
                log_message(f"Último hash encontrado no log: {last_hash}")
                return last_hash
    log_message("Nenhum hash encontrado no log anterior.")
    return None

def fetch_data_and_compare(**kwargs):
    log_message("Download dos dados para verificação usando hash.")
    response = requests.get(url)
    response.raise_for_status()
    breweries = response.json()

    breweries_sorted_str = json.dumps(breweries, sort_keys=True)
    new_data_hash = hashlib.md5(breweries_sorted_str.encode('utf-8')).hexdigest()
    log_message(f"Hash dos dados atuais baixados: {new_data_hash}")

    last_hash = get_last_hash_from_log()

    if last_hash == new_data_hash:
        log_message("Nenhuma atualização detectada. Dados inalterados.")
        save_log()
        kwargs['ti'].xcom_push(key='file_updated', value=False)
        log_message("Enviando XCom: Sinal para que a DAG Silver NÃO seja executada.")
        return False

    upload_to_gcs(breweries_sorted_str, new_data_hash)
    kwargs['ti'].xcom_push(key='file_updated', value=True)
    log_message("Enviando XCom: Sinal para que a DAG Silver seja executada.")
    return True

def upload_to_gcs(breweries_data, new_data_hash):
    log_message("Atualização detectada. Realizando upload dos dados para o GCS.")
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(breweries_data, content_type='application/json')
    log_message(f"Bucket de destino: {bucket_name}, Caminho do arquivo: {blob_name}")
    save_log(new_data_hash)

def save_log(new_data_hash=None):
    if new_data_hash:
        log_message(f"Novo hash dos dados armazenados: {new_data_hash}")

    client = storage.Client()
    log_bucket = client.get_bucket(log_bucket_name)
    log_blob = log_bucket.blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_content = "\n".join(log_messages).encode('utf-8')
    log_blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
    logging.info("Log salvo com sucesso no bucket de logs.")

# Definindo os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure
}

# Definindo a DAG Bronze
with DAG(
    'bronze_dag',
    default_args=default_args,
    description='DAG para verificar atualização e consumir dados da API Open Brewery DB',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_and_compare',
        python_callable=fetch_data_and_compare,
        provide_context=True
    )
