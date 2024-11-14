import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from google.cloud import storage

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['your_email@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_all_breweries():
    logging.info("Iniciando a coleta de dados da API Open Brewery DB")
    all_breweries = []
    page = 1
    while True:
        url = f"https://api.openbrewerydb.org/breweries?page={page}&per_page=50"
        logging.info(f"Fazendo requisição para a página {page}")
        response = requests.get(url)
        response.raise_for_status()
        breweries = response.json()
        if not breweries:
            logging.info("Nenhuma nova cervejaria encontrada, terminando a coleta.")
            break
        all_breweries.extend(breweries)
        logging.info(f"{len(breweries)} registros adicionados")
        page += 1
    
    logging.info("Finalizando coleta de dados, enviando para o bucket GCS na camada Bronze")
    
    # Salvar os dados no GCS na camada Bronze
    client = storage.Client()
    bucket = client.get_bucket('bucket-case-abinbev')
    blob = bucket.blob('data/bronze/breweries_raw.json')
    blob.upload_from_string(json.dumps(all_breweries), content_type='application/json')

    logging.info("Arquivo enviado para o bucket GCS com sucesso")

with DAG(
    'bronze_dag',
    default_args=default_args,
    description='DAG para consumir dados da API Open Brewery DB e salvar na camada Bronze',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_breweries_data',
        python_callable=fetch_all_breweries,
    )

    fetch_data
