import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import pandas as pd

# Configuração de e-mail para falhas
def alert_email_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
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

# Função para salvar logs
def save_log(messages):
    try:
        client = storage.Client()
        log_bucket_name = "us-central1-composer-case-e66c77cc-bucket"
        log_bucket = client.get_bucket(log_bucket_name)
        
        log_blob_name = f'logs/gold_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log'
        log_blob = log_bucket.blob(log_blob_name)
        
        log_content = "\n".join(messages)
        log_blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
        
        logging.info(f"Log salvo com sucesso no bucket {log_bucket_name} com nome {log_blob_name}")
    except Exception as e:
        logging.error(f"Erro ao salvar log no bucket de logs: {e}")
        raise

# Callback para salvar logs em caso de falha
def save_log_on_failure(context):
    log_messages = [
        f"Erro na DAG: {context['dag'].dag_id}",
        f"Task que falhou: {context['task_instance'].task_id}",
        f"Data de execução: {context['execution_date']}",
        f"Mensagem de erro: {context['exception']}",
    ]
    save_log(log_messages)

# Sensor para verificar sucesso da Silver
def check_silver_success(**kwargs):
    signal = kwargs['ti'].xcom_pull(
        dag_id='silver_dag',
        task_ids='load_data_to_bigquery',
        key='success_signal'
    )
    if signal:
        logging.info("Recebido sinal de sucesso da DAG Silver.")
        return True
    else:
        logging.info("Nenhum sinal de sucesso recebido da DAG Silver.")
        return False

# Transformação dos dados para a camada Gold
def transform_to_gold(**kwargs):
    log_messages = ["Iniciando a transformação dos dados para a camada Gold"]
    try:
        bucket_name = "bucket-case-abinbev"
        silver_file_path = "data/silver/breweries_transformed/breweries_transformed.parquet"
        gold_file_path = "data/gold/breweries_aggregated.parquet"
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        
        silver_blob = bucket.blob(silver_file_path)
        silver_data = silver_blob.download_as_bytes()
        
        silver_df = pd.read_parquet(silver_data)
        gold_df = silver_df.groupby(["country", "state", "brewery_type"]).size().reset_index(name="total_breweries")
        
        gold_df.to_parquet(gold_file_path, index=False)
        gold_blob = bucket.blob(gold_file_path)
        gold_blob.upload_from_filename(gold_file_path)
        
        log_messages.append("Transformação e salvamento no GCS concluídos com sucesso.")
    except Exception as e:
        log_messages.append(f"Erro durante a transformação para a camada Gold: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Carregar dados no BigQuery
def load_gold_to_bigquery_from_parquet(**kwargs):
    log_messages = ["Iniciando o carregamento dos dados da camada Gold para o BigQuery"]
    try:
        bucket_name = "bucket-case-abinbev"
        gold_file_path = "data/gold/breweries_aggregated.parquet"
        
        client = bigquery.Client()
        table_id = "case-abinbev.Medallion.gold"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        uri = f"gs://{bucket_name}/{gold_file_path}"
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        
        log_messages.append(f"Dados carregados com sucesso para a tabela {table_id}.")
    except Exception as e:
        log_messages.append(f"Erro ao carregar os dados no BigQuery: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Configuração da DAG Gold
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': save_log_on_failure,
}

with DAG(
    'gold_dag',
    default_args=default_args,
    description='DAG para transformar dados da camada Silver e carregar na camada Gold no BigQuery',
    schedule_interval=None,  # Disparada somente pela Silver
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Validação do sucesso da Silver
    wait_for_silver_success = PythonSensor(
        task_id="wait_for_silver_success",
        python_callable=check_silver_success,
        poke_interval=30,
        timeout=150,  # Limita o poking em 5 tentativas (150 segundos no total)
        mode="poke"
    )
    
    # Transformação para Gold
    transform_to_gold_task = PythonOperator(
        task_id="transform_to_gold",
        python_callable=transform_to_gold,
    )
    
    # Carregamento no BigQuery
    load_gold_to_bigquery_task = PythonOperator(
        task_id="load_gold_to_bigquery_from_parquet",
        python_callable=load_gold_to_bigquery_from_parquet,
    )
    
    # Fluxo da DAG
    wait_for_silver_success >> transform_to_gold_task >> load_gold_to_bigquery_task
