import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

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

# Sensor para verificar se o arquivo foi atualizado na Bronze
def check_file_updated(**kwargs):
    updated = kwargs['ti'].xcom_pull(
        dag_id='bronze_dag',  # Nome da DAG Bronze
        task_ids='fetch_data_and_compare',  # Task que define o XCom
        key='file_updated'
    )
    if updated:
        logging.info("Recebido sinal no XCom para executar a DAG Silver: Executando.")
        return True
    else:
        logging.info("Recebido sinal no XCom para NÃO executar a DAG Silver: Abortando.")
        return False

# Função para baixar os dados do GCS
def download_data(**kwargs):
    log_messages = ["Iniciando o download dos dados da camada Bronze"]
    try:
        bucket_name = "bucket-case-abinbev"
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob("data/bronze/breweries_raw.json")
        raw_data = blob.download_as_text()
        kwargs['ti'].xcom_push(key="raw_data", value=raw_data)
        log_messages.append("Download concluído com sucesso.")
    except Exception as e:
        log_messages.append(f"Erro ao baixar os dados: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para transformar os dados
def transform_data(**kwargs):
    log_messages = ["Iniciando a transformação dos dados"]
    try:
        raw_data = kwargs['ti'].xcom_pull(key="raw_data", task_ids="download_data")
        raw_df = pd.read_json(raw_data)
        raw_df["id"] = raw_df["id"].astype(str).str.strip()
        raw_df["name"] = raw_df["name"].astype(str).str.title()
        raw_df["brewery_type"] = raw_df["brewery_type"].fillna("unknown").astype(str)
        raw_df["state_partition"] = raw_df["state"].apply(lambda x: hash(x) % 50)
        kwargs['ti'].xcom_push(key="transformed_data", value=raw_df.to_json())
        log_messages.append("Transformação concluída com sucesso.")
    except Exception as e:
        log_messages.append(f"Erro na transformação dos dados: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para carregar os dados no BigQuery
def load_data_to_bigquery(**kwargs):
    log_messages = ["Iniciando o carregamento dos dados para o BigQuery"]
    try:
        transformed_data = kwargs['ti'].xcom_pull(key="transformed_data", task_ids="transform_data")
        transformed_df = pd.read_json(transformed_data)
        project_id = "case-abinbev"
        dataset_id = "Medallion"
        table_id = "silver"
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(transformed_df, table_ref, job_config=job_config)
        job.result()
        log_messages.append(f"Dados carregados com sucesso na tabela {dataset_id}.{table_id} no BigQuery.")
    except Exception as e:
        log_messages.append(f"Erro ao carregar os dados no BigQuery: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para salvar logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket("us-central1-composer-case-e66c77cc-bucket")
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
    description='DAG para transformar dados da camada Bronze e carregar na camada Silver no BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Sensor para verificar se a DAG Bronze detectou atualização
    wait_for_file_update = PythonSensor(
        task_id='wait_for_file_update',
        python_callable=check_file_updated,
        poke_interval=30,
        timeout=3600,
        mode='poke',
    )
    
    # Task de download
    download_data_task = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )
    
    # Task de transformação
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )
    
    # Task de carregamento
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery,
    )

    # Definindo a sequência de execução
    wait_for_file_update >> download_data_task >> transform_data_task >> load_data_task
