import logging
import json
import requests
import hashlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage

# Função de callback para enviar e-mail em caso de falha
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

# Função principal da DAG Bronze
def fetch_and_check_breweries():
    log_messages = []
    log_messages.append(f"Execução da DAG: {datetime.utcnow().isoformat()}")

    try:
        # Definindo o endpoint e verificando atualização
        url = "https://api.openbrewerydb.org/breweries"
        response = requests.head(url)
        response.raise_for_status()

        # Tentativa de verificação pelo cabeçalho Last-Modified
        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            last_modified_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            log_messages.append(f"Tentativa de verificação de atualização usando Last-Modified: {last_modified}")
            
            # Conexão com o GCS e verificação usando Last-Modified
            client = storage.Client()
            bucket = client.get_bucket('bucket-case-abinbev')
            blob = bucket.blob('data/bronze/breweries_raw.json')
            
            if blob.exists() and blob.metadata:
                gcs_last_update = blob.metadata.get("last_update")
                if gcs_last_update:
                    gcs_last_update_date = datetime.strptime(gcs_last_update, "%Y-%m-%dT%H:%M:%S%Z")
                    if last_modified_date <= gcs_last_update_date:
                        log_messages.append("Nenhuma atualização detectada usando Last-Modified. Arquivo no bucket GCS e tabela BigQuery permanecem inalterados.")
                        save_log(log_messages)
                        return
            log_messages.append("Atualização detectada pelo Last-Modified. Baixando e atualizando arquivo JSON no bucket.")

        else:
            log_messages.append("Cabeçalho Last-Modified não encontrado. Tentando verificação por hash.")

            # Fazendo requisição para obter dados completos e calcular o hash
            response = requests.get(url)
            response.raise_for_status()
            breweries = response.json()
            
            # Calculando o hash dos dados JSON atuais
            new_data_hash = hashlib.md5(json.dumps(breweries, sort_keys=True).encode('utf-8')).hexdigest()

            # Verificação de atualização usando hash
            client = storage.Client()
            bucket = client.get_bucket('bucket-case-abinbev')
            blob = bucket.blob('data/bronze/breweries_raw.json')

            if blob.exists() and blob.metadata:
                gcs_last_hash = blob.metadata.get("data_hash")
                if gcs_last_hash == new_data_hash:
                    log_messages.append("Nenhuma atualização detectada usando hash. Arquivo no bucket GCS e tabela BigQuery permanecem inalterados.")
                    save_log(log_messages)
                    return

            log_messages.append("Atualização detectada pelo hash. Baixando e atualizando arquivo JSON no bucket.")
        
        # Preparação dos dados para upload
        json_lines = "\n".join([json.dumps(brewery) for brewery in breweries])

        # Upload dos dados para o GCS e atualização de metadados
        blob.upload_from_string(json_lines, content_type='application/json')
        blob.metadata = {
            "last_update": last_modified_date.strftime("%Y-%m-%dT%H:%M:%S%Z") if last_modified else None,
            "data_hash": new_data_hash
        }
        blob.patch()
        
        # Mensagem clara indicando a atualização no bucket e na tabela BigQuery
        log_messages.append("Arquivo JSON atualizado com sucesso no bucket GCS.")
        log_messages.append("Atualizando dados na tabela BigQuery: bronze table ID: case-abinbev.Medallion.bronze com os dados mais recentes.")

    except requests.exceptions.RequestException as e:
        log_messages.append(f"Erro ao acessar o endpoint: {e}")
        logging.error(f"Erro ao acessar o endpoint: {e}")
        raise

    except Exception as e:
        log_messages.append(f"Erro ao salvar no bucket GCS: {e}")
        logging.error(f"Erro ao salvar no bucket GCS: {e}")
        raise

    save_log(log_messages)

# Função para salvar o log no bucket de logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket('us-central1-composer-case-e66c77cc-bucket')
    log_blob = log_bucket.blob(f'logs/bronze_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    
    # Concatene as mensagens e defina o encoding explicitamente
    log_content = "\n".join(messages).encode('utf-8')
    log_blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
    logging.info("Log salvo com sucesso no bucket de logs.")

# Definindo os argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure  # Callback para falha
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
    
    fetch_data = PythonOperator(
        task_id='fetch_and_check_breweries',
        python_callable=fetch_and_check_breweries,
    )

    fetch_data
