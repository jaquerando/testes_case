import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

# Função para carregar os dados transformados no BigQuery
def load_data_to_bigquery():
    log_messages = ["Iniciando transformação e carregamento dos dados para BigQuery"]
    
    try:
        # Configuração do caminho no GCS
        bucket_name = "bucket-case-abinbev"
        bronze_path = f"gs://{bucket_name}/data/bronze/breweries_raw.json"
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        
        # Lendo os dados do JSON
        blob = bucket.blob("data/bronze/breweries_raw.json")
        raw_data = blob.download_as_text()
        raw_df = pd.read_json(raw_data)

        # Transformações nos dados
        raw_df["id"] = raw_df["id"].str.strip()
        raw_df["name"] = raw_df["name"].str.title()
        raw_df["brewery_type"] = raw_df["brewery_type"].fillna("unknown")
        raw_df["address_1"] = raw_df["address_1"].str.title()
        raw_df["city"] = raw_df["city"].str.title()
        raw_df["state"] = raw_df["state"].str.lower()
        raw_df["state_partition"] = raw_df["state"].apply(lambda x: hash(x) % 50)  # Calcula o particionamento
        raw_df["country"] = raw_df["country"].str.title()
        raw_df["longitude"] = pd.to_numeric(raw_df["longitude"], errors="coerce")
        raw_df["latitude"] = pd.to_numeric(raw_df["latitude"], errors="coerce")
        raw_df["phone"] = raw_df["phone"].str.strip()
        raw_df["website_url"] = raw_df["website_url"].str.strip()

        log_messages.append("Dados transformados com sucesso.")

        # Configuração do BigQuery
        project_id = "case-abinbev"
        dataset_id = "Medallion"
        table_id = "silver"
        
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)

        # Carregando os dados no BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = client.load_table_from_dataframe(raw_df, table_ref, job_config=job_config)
        job.result()  # Aguarda o término do job

        log_messages.append(f"Dados carregados com sucesso na tabela {dataset_id}.{table_id} no BigQuery.")

    except Exception as e:
        log_messages.append(f"Erro ao carregar os dados: {e}")
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
    
    # Task de transformação e carregamento
    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery,
    )
