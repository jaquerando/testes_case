import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from google.cloud import bigquery, storage
from datetime import datetime, timedelta
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

# Função para transformar os dados na camada Gold
def transform_to_gold(**kwargs):
    log_messages = ["Iniciando a transformação dos dados para a camada Gold"]
    try:
        # Configuração do BigQuery
        client = bigquery.Client()
        query = """
            SELECT
                country,
                brewery_type,
                COUNT(*) AS total_breweries
            FROM `case-abinbev.Medallion.silver`
            GROUP BY country, brewery_type
        """
        job_config = bigquery.QueryJobConfig(destination="case-abinbev.Medallion.gold")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        # Executando a consulta e salvando na tabela Gold
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        log_messages.append("Transformação e carregamento para a camada Gold concluídos com sucesso.")
    except Exception as e:
        log_messages.append(f"Erro ao transformar e carregar os dados: {e}")
        logging.error(f"Erro: {e}")
        raise
    save_log(log_messages)

# Função para salvar logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket("us-central1-composer-case-e66c77cc-bucket")
    log_blob = log_bucket.blob(f'logs/gold_dag_log_{datetime.utcnow().strftime("%Y%m%d%H%M%S")}.log')
    log_content = "\n".join(messages).encode("utf-8")
    log_blob.upload_from_string(log_content, content_type="text/plain; charset=utf-8")
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
    
    # Task de transformação e carregamento
    transform_to_gold_task = PythonOperator(
        task_id='transform_to_gold',
        python_callable=transform_to_gold,
        provide_context=True,
    )
