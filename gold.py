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

# Sensor para verificar se a DAG Silver foi executada e finalizada com sucesso
def check_silver_updated(**kwargs):
    updated = kwargs['ti'].xcom_pull(
        dag_id='silver_dag',  # Nome da DAG Silver
        task_ids='load_data_to_bigquery',  # Task que envia o XCom
        key='file_updated'
    )
    if updated:
        logging.info("Recebido sinal no XCom para executar a DAG Gold: Executando.")
        return True
    else:
        logging.info("Recebido sinal no XCom para NÃO executar a DAG Gold: Abortando.")
        return False

# Função para transformar os dados
def transform_to_gold(**kwargs):
    log_messages = ["Iniciando a transformação dos dados para a camada Gold"]
    try:
        # Conecta ao GCS e lê o arquivo Parquet da camada Silver
        bucket_name = "bucket-case-abinbev"
        silver_file_path = "data/silver/breweries_transformed/breweries_transformed.parquet"

        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(silver_file_path)
        blob_data = blob.download_as_bytes()

        # Lê o Parquet como DataFrame
        silver_df = pd.read_parquet(blob_data)
        gold_df = (
            silver_df.groupby(["country", "state", "brewery_type"])
            .size()
            .reset_index(name="total_breweries")
        )

        # Salva o DataFrame resultante em Parquet no GCS
        gold_file_path = "data/gold/breweries_aggregated.parquet"
        gold_df.to_parquet(f"gs://{bucket_name}/{gold_file_path}", index=False)
        kwargs['ti'].xcom_push(key="gold_file_path", value=gold_file_path)
        log_messages.append("Transformação e carregamento para a camada Gold concluídos com sucesso.")
    except Exception as e:
        log_messages.append(f"Erro na transformação dos dados para a camada Gold: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para carregar os dados no BigQuery
def load_gold_to_bigquery(**kwargs):
    log_messages = ["Iniciando o carregamento dos dados para o BigQuery"]
    try:
        gold_file_path = kwargs['ti'].xcom_pull(key="gold_file_path", task_ids="transform_to_gold")
        project_id = "case-abinbev"
        dataset_id = "Medallion"
        table_id = "gold"
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)

        # Configuração de carregamento
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Carregar o arquivo Parquet do GCS para o BigQuery
        uri = f"gs://{gold_file_path}"
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()

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
    log_blob = log_bucket.blob(f'logs/gold_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_blob.upload_from_string("\n".join(messages), content_type="text/plain; charset=utf-8")
    logging.info("Log salvo no bucket de logs.")

# Configuração da DAG Gold
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alert_email_on_failure
}

with DAG(
    'gold_dag',
    default_args=default_args,
    description='DAG para agregar dados da camada Silver e carregar na camada Gold no BigQuery',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Sensor para verificar se a DAG Silver detectou atualização
    wait_for_silver_update = PythonSensor(
        task_id='wait_for_silver_update',
        python_callable=check_silver_updated,
        poke_interval=30,  # Intervalo entre tentativas (30 segundos)
        timeout=150,  # Máximo de 5 tentativas
        mode='poke',  # Aguarda a condição antes de prosseguir
    )

    # Task de transformação
    transform_to_gold_task = PythonOperator(
        task_id='transform_to_gold',
        python_callable=transform_to_gold,
    )

    # Task de carregamento
    load_gold_task = PythonOperator(
        task_id='load_gold_to_bigquery',
        python_callable=load_gold_to_bigquery,
    )

    # Definindo a sequência de execução
    wait_for_silver_update >> transform_to_gold_task >> load_gold_task
