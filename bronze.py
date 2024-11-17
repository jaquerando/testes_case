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
data_blob_name = 'data/bronze/breweries_raw.ndjson'
control_blob_name = 'data/bronze/last_update.txt'
log_bucket_name = 'us-central1-composer-case-e66c77cc-bucket'
log_messages = []

def log_message(message):
    log_messages.append(f"\n{message}\n{'-'*40}")

def get_last_update_from_control_file():
    """Lê o arquivo de controle para obter o hash ou timestamp da última atualização."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    control_blob = bucket.blob(control_blob_name)
    
    if not control_blob.exists():
        log_message("Arquivo de controle não encontrado. Considerando como primeira execução.")
        return None

    last_update = control_blob.download_as_text().strip()
    log_message(f"Último hash/timestamp encontrado no arquivo de controle: {last_update}")
    return last_update

def save_to_control_file(content):
    """Atualiza o arquivo de controle com o novo hash/timestamp."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    control_blob = bucket.blob(control_blob_name)
    control_blob.upload_from_string(content)
    log_message(f"Arquivo de controle atualizado com: {content}")

def fetch_data_and_compare():
    """Realiza o download dos dados e compara com o último hash/timestamp armazenado."""
    log_message("Download dos dados para verificação.")
    response = requests.get(url)
    response.raise_for_status()
    breweries = response.json()
    
    # Ordena os dados e converte em string antes de calcular o hash
    breweries_sorted_str = json.dumps(breweries, sort_keys=True)
    current_hash = hashlib.md5(breweries_sorted_str.encode('utf-8')).hexdigest()
    log_message(f"Hash dos dados atuais baixados: {current_hash}")

    # Obtém o último hash/timestamp do arquivo de controle
    last_update = get_last_update_from_control_file()

    if last_update == current_hash:
        log_message("Nenhuma atualização detectada ao comparar com o arquivo de controle.")
        save_log()
        return False  # Dados inalterados

    # Caso haja diferença, salva os novos dados no GCS e atualiza o controle
    save_data_to_gcs(breweries_sorted_str, current_hash)
    save_to_control_file(current_hash)
    return True  # Dados atualizados

def save_data_to_gcs(data, current_hash):
    """Faz o upload dos dados para o GCS."""
    log_message("Atualização detectada. Realizando upload dos dados para o GCS.")
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(data_blob_name)

    # Formata os dados para NDJSON
    breweries_list = json.loads(data)
    formatted_data = "\n".join([json.dumps(record) for record in breweries_list])
    blob.upload_from_string(formatted_data, content_type='application/x-ndjson')

    log_message(f"Bucket de destino: {bucket_name}, Caminho do arquivo: {data_blob_name}")
    log_message(f"Novo hash armazenado no controle: {current_hash}")
    save_log()

def save_log():
    """Salva as mensagens do log no bucket de logs."""
    client = storage.Client()
    log_bucket = client.get_bucket(log_bucket_name)
    log_blob = log_bucket.blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_blob.upload_from_string("\n".join(log_messages), content_type="text/plain")
    logging.info("Log salvo no bucket de logs.")

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
        python_callable=fetch_data_and_compare
    )
