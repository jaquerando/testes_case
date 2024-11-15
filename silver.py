import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from google.cloud import storage
import great_expectations as ge
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, initcap, coalesce

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

# Função para transformar dados e salvar na camada Silver
def transform_data_to_silver():
    log_messages = ["Iniciando transformação dos dados da camada Bronze para Silver"]
    
    try:
        spark = SparkSession.builder.appName("BrewerySilverLayer").getOrCreate()
        log_messages.append("Sessão Spark inicializada")
        
        bronze_path = "gs://bucket-case-abinbev/data/bronze/breweries_raw.json"
        silver_path = "gs://bucket-case-abinbev/data/silver/breweries_transformed"
        
        raw_df = spark.read.json(bronze_path)
        log_messages.append("Dados carregados da camada Bronze")

        # Transformações completas nos dados
        transformed_df = (
            raw_df
            .withColumn("id", trim(col("id")))
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("brewery_type", coalesce(col("brewery_type"), col("unknown")))
            .withColumn("address_1", initcap(trim(col("address_1"))))
            .withColumn("address_2", initcap(trim(col("address_2"))))
            .withColumn("address_3", initcap(trim(col("address_3"))))
            .withColumn("city", initcap(trim(col("city"))))
            .withColumn("state_province", lower(trim(col("state_province"))))
            .withColumn("postal_code", trim(col("postal_code")))
            .withColumn("country", initcap(trim(col("country"))))
            .withColumn("longitude", col("longitude").cast("double"))
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("phone", trim(col("phone")))
            .withColumn("website_url", trim(col("website_url")))
            .withColumn("state", lower(trim(col("state"))))
            .withColumn("street", initcap(trim(col("street"))))
            .na.drop(subset=["id", "name", "city", "state", "country"])
        )
        
        transformed_df.write.mode("overwrite").partitionBy("country").parquet(silver_path)
        log_messages.append("Dados salvos na camada Silver")

    except Exception as e:
        log_messages.append(f"Erro na transformação: {e}")
        logging.error(f"Erro: {e}")
        raise

    save_log(log_messages)

# Função para executar validações com Great Expectations
def validate_data_with_great_expectations():
    silver_path = "gs://bucket-case-abinbev/data/silver/breweries_transformed"
    
    try:
        # Configuração do Great Expectations para validação de dados
        context = ge.get_context()
        df = ge.read_parquet(silver_path)
        ge_df = ge.from_pandas(df)
        
        # Definindo expectativas para todos os campos importantes
        ge_df.expect_column_values_to_not_be_null("id")
        ge_df.expect_column_values_to_not_be_null("name")
        ge_df.expect_column_values_to_not_be_null("city")
        ge_df.expect_column_values_to_not_be_null("state")
        ge_df.expect_column_values_to_not_be_null("country")
        ge_df.expect_column_values_to_be_of_type("longitude", "FLOAT")
        ge_df.expect_column_values_to_be_of_type("latitude", "FLOAT")
        ge_df.expect_column_values_to_match_regex("postal_code", r"^\d{5}(-\d{4})?$")  # Exemplo de validação de código postal
        ge_df.expect_column_values_to_be_of_type("phone", "STRING")
        ge_df.expect_column_values_to_match_regex("website_url", r"^https?://")
        
        # Validar dados
        results = ge_df.validate()
        
        if not results["success"]:
            raise ValueError("Falha na validação dos dados: expectativas não atendidas")

    except Exception as e:
        logging.error(f"Erro na validação com Great Expectations: {e}")
        raise

# Função para salvar logs no bucket de logs
def save_log(messages):
    client = storage.Client()
    log_bucket = client.get_bucket('us-central1-composer-case-e66c77cc-bucket')
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
    description='DAG para transformar dados da camada Bronze e salvar na camada Silver',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    transform_data = PythonOperator(
        task_id='transform_data_to_silver',
        python_callable=transform_data_to_silver,
    )
    
    validate_data = PythonOperator(
        task_id='validate_data_with_great_expectations',
        python_callable=validate_data_with_great_expectations,
    )

    transform_data >> validate_data
