import logging
import json
import requests
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage

# Configurações de e-mail para falhas
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

# Inicializa variáveis globais
url = "https://api.openbrewerydb.org/breweries"
client = storage.Client()
bucket_name = 'bucket-case-abinbev'
blob_name = 'data/bronze/breweries_raw.json'
log_messages = []

def log_message(message):
    log_messages.append(message)

def check_last_modified():
    log_message("###############################\n   INÍCIO DA DAG\n###############################")
    log_message(f"Verificando o endpoint {url} para dados de atualização.\n")
    
    # Verifica o cabeçalho Last-Modified
    response = requests.head(url)
    response.raise_for_status()
    last_modified = response.headers.get("Last-Modified")
    
    if last_modified:
        last_modified_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
        log_message(f"Tentativa de verificação usando Last-Modified: {last_modified}")
        
        # Conexão com o bucket e blob do GCS
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Verifica se o arquivo no GCS já está atualizado
        if blob.exists() and blob.metadata:
            gcs_last_update = blob.metadata.get("last_update")
            if gcs_last_update:
                gcs_last_update_date = datetime.strptime(gcs_last_update, "%Y-%m-%dT%H:%M:%SZ")
                if last_modified_date <= gcs_last_update_date:
                    log_message("Nenhuma atualização detectada usando Last-Modified. Dados inalterados.")
                    save_log(log_messages)
                    return "skip"
        log_message("Atualização detectada pelo Last-Modified. Baixando e atualizando arquivo JSON no bucket.")
    else:
        log_message("Cabeçalho Last-Modified não encontrado. Tentando verificação por hash.")
    
    return "proceed"

def fetch_data_and_hash():
    # Baixa os dados completos e calcula o hash
    log_message("Obtendo dados completos do endpoint para calcular o hash.")
    response = requests.get(url)
    response.raise_for_status()
    breweries = response.json()
    
    # Calcula o hash dos dados JSON atuais
    new_data_hash = hashlib.md5(json.dumps(breweries, sort_keys=True).encode('utf-8')).hexdigest()
    log_message(f"Hash dos dados atuais: {new_data_hash}")

    return {
        "breweries": breweries,
        "new_data_hash": new_data_hash
    }

def compare_and_upload(ti):
    # Recupera dados e hash da task anterior
    breweries = ti.xcom_pull(task_ids='fetch_data_and_hash')['breweries']
    new_data_hash = ti.xcom_pull(task_ids='fetch_data_and_hash')['new_data_hash']

    # Conexão com o GCS
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Compara o hash atual com o hash armazenado nos metadados do blob
    if blob.exists() and blob.metadata:
        gcs_last_hash = blob.metadata.get("data_hash")
        log_message(f"Comparando hash atual ({new_data_hash}) com hash armazenado no GCS ({gcs_last_hash})")
        
        if gcs_last_hash == new_data_hash:
            log_message("Nenhuma atualização detectada usando hash. Dados inalterados.")
            save_log(log_messages)
            return "skip"
    
    # Prepara e faz o upload dos dados para o GCS
    json_lines = "\n".join([json.dumps(brewery) for brewery in breweries])
    blob.upload_from_string(json_lines, content_type='application/json')
    blob.metadata = {
        "last_update": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_hash": new_data_hash  # Armazena o hash como metadado
    }
    blob.patch()

    log_message("Arquivo JSON atualizado com sucesso no bucket GCS.")
    log_message(f"Bucket destino: {bucket_name}, arquivo: {blob_name}")
    log_message("Dados atualizados na tabela BigQuery: bronze table ID: case-abinbev.Medallion.bronze")
    save_log(log_messages)

# Função para salvar o log no bucket de logs
def save_log(messages):
    log_bucket = client.get_bucket('us-central1-composer-case-e66c77cc-bucket')
    log_blob = log_bucket.blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_content = "\n\n".join(messages).encode('utf-8')
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
    
    check_last_modified_task = PythonOperator(
        task_id='check_last_modified',
        python_callable=check_last_modified
    )

    fetch_data_and_hash_task = PythonOperator(
        task_id='fetch_data_and_hash',
        python_callable=fetch_data_and_hash
    )

    compare_and_upload_task = PythonOperator(
        task_id='compare_and_upload',
        python_callable=compare_and_upload
    )

    # Encadeamento das tasks com condição para prosseguir
    check_last_modified_task >> fetch_data_and_hash_task >> compare_and_upload_task
