import logging
import json
import requests
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
        
        # Verificação do cabeçalho Last-Modified
        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            last_modified_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            log_messages.append(f"Última modificação no endpoint: {last_modified}")
        else:
            log_messages.append("Cabeçalho Last-Modified não encontrado. Atualização forçada.")
            last_modified_date = datetime.utcnow()

        # Conexão com o GCS e verificação de atualização
        client = storage.Client()
        bucket = client.get_bucket('bucket-case-abinbev')
        blob = bucket.blob('data/bronze/breweries_raw.json')

        if blob.exists():
            gcs_last_update = blob.metadata.get("last_update")
            if gcs_last_update:
                gcs_last_update_date = datetime.strptime(gcs_last_update, "%Y-%m-%dT%H:%M:%SZ")
                if last_modified_date <= gcs_last_update_date:
                    log_messages.append("Nenhuma atualização detectada no endpoint. Dados não atualizados no bucket.")
                    save_log(log_messages)
                    return

        # Baixando e salvando os dados atualizados
        log_messages.append("Atualização detectada. Iniciando o download.")
        response = requests.get(url)
        response.raise_for_status()
        breweries = response.json()
        json_lines = "\n".join([json.dumps(brewery) for brewery in breweries])

        # Upload dos dados para o GCS
        blob.upload_from_string(json_lines, content_type='application/json')
        blob.metadata = {"last_update": last_modified_date.strftime("%Y-%m-%dT%H:%M:%SZ")}
        blob.patch()
        log_messages.append("Arquivo atualizado com sucesso no bucket GCS.")
        log_messages.append("Atualizando a tabela BigQuery: bronze table ID: case-abinbev.Medallion.endpoint_not")

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
    log_content = "\n".join(messages)
    log_blob.upload_from_string(log_content, content_type="text/plain")
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
