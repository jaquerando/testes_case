import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
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

# Sensor para verificar execução da Silver com limite de poking
def check_silver_execution(**kwargs):
    ti = kwargs['ti']
    # Número máximo de tentativas
    max_poking_attempts = 20

    # Recupera o contador de tentativas atual
    current_attempt = ti.xcom_pull(key="poking_attempt", task_ids="wait_for_silver_execution") or 0

    if current_attempt >= max_poking_attempts:
        logging.info(f"Limite de poking alcançado ({max_poking_attempts} tentativas). Abortando o sensor.")
        return True  # Finaliza o sensor

    # Atualiza o contador de tentativas no XCom
    ti.xcom_push(key="poking_attempt", value=current_attempt + 1)

    # Verifica se a Silver foi executada
    updated = ti.xcom_pull(
        dag_id='silver_dag',
        task_ids='load_data_to_bigquery',  # Task final da DAG Silver
        key='return_value'
    )

    if updated:
        logging.info("Recebido sinal no XCom para executar a DAG Gold: Executando.")
        return True
    else:
        logging.info(f"Tentativa {current_attempt + 1}: Recebido sinal no XCom para NÃO executar a DAG Gold.")
        return False

# Função para transformar os dados
def transform_to_gold(**kwargs):
    log_messages = ["Iniciando a transformação dos dados para a camada Gold"]
    try:
        # Configuração de cliente GCS
        bucket_name = "bucket-case-abinbev"
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob("data/silver/breweries_transformed.json")
        silver_data = blob.download_as_text()

        # Carregando os dados da camada Silver
        silver_df = pd.read_json(silver_data, lines=True)

        # Transformações para camada Gold
        gold_df = silver_df.groupby(["country", "brewery_type"]).size().reset_index(name="count_breweries")

        # Salvando arquivo no GCS
        gold_blob = bucket.blob("data/gold/breweries_aggregated.json")
        gold_blob.upload_from_string(gold_df.to_json(orient="records", lines=True), content_type="application/json")
        log_messages.append("Arquivo transformado e salvo no bucket na camada Gold.")
        kwargs['ti'].xcom_push(key="gold_data", value=gold_df.to_json(orient="records"))
    except Exception as e:
        log_messages.append(f"Erro na transformação dos dados para a camada Gold: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para carregar os dados no BigQuery
def load_gold_to_bigquery(**kwargs):
    log_messages = ["Iniciando o carregamento dos dados para a camada Gold"]
    try:
        gold_data = kwargs['ti'].xcom_pull(key="gold_data", task_ids="transform_to_gold")
        gold_df = pd.read_json(gold_data)

        # Configurações do BigQuery
        project_id = "case-abinbev"
        dataset_id = "Medallion"
        table_id = "gold"
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(gold_df, table_ref, job_config=job_config)
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
    log_blob = log_bucket.blob(f'logs/gold_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_blob.upload_from_string("\n".join(messages).encode("utf-8"), content_type="text/plain; charset=utf-8")
    logging.info("Log salvo no bucket de logs com encoding UTF-8.")

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
    description='DAG para transformar dados da camada Silver e carregar na camada Gold no BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Sensor para verificar execução da Silver
    wait_for_silver_execution = PythonSensor(
        task_id='wait_for_silver_execution',
        python_callable=check_silver_execution,
        poke_interval=30,
        timeout=3600,
        mode='poke',
    )
    
    # Task de transformação
    transform_to_gold_task = PythonOperator(
        task_id='transform_to_gold',
        python_callable=transform_to_gold,
        provide_context=True,
    )
    
    # Task de carregamento
    load_gold_to_bigquery_task = PythonOperator(
        task_id='load_gold_to_bigquery',
        python_callable=load_gold_to_bigquery,
        provide_context=True,
    )

    # Definindo a sequência de execução
    wait_for_silver_execution >> transform_to_gold_task >> load_gold_to_bigquery_task
