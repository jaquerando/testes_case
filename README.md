# Data pipeline summary

This data pipeline ingests data from the Open Brewery DB API, performs transformations, and loads it into a data lake structured with Bronze, Silver, and Gold layers. 
It leverages Apache Airflow for orchestration, ensuring scheduled execution, error handling, and dependency management between the stages. 
Google Cloud Storage is used for data storage, and BigQuery serves as the final destination for analytical queries.

Medallion Architecture: Implements the Bronze, Silver, Gold layered approach for data lake organization.
Incremental Loading: Only processes new or changed data, improving efficiency.
Data Validation: Uses hash comparisons to ensure data integrity.
Monitoring and Alerting: Includes logging to GCS and email alerts for proactive monitoring.
Orchestration: Utilizes Apache Airflow for scheduling, task dependencies, and error handling.

Big Query Interface:

![image](https://github.com/user-attachments/assets/cf30ebf1-388e-4ebc-918c-daeb0ee647ca)

# Total cost of pipeline = R$ 471,35

Up to Nov 19th, total spent was around R$ 471,35, with Cloud Composer (R$ 467,53) and Cloud Storage R$ 3,82.

![image](https://github.com/user-attachments/assets/52dd34ec-2be2-442a-aebe-244c92ec6893)

![image](https://github.com/user-attachments/assets/e741493b-6617-40f9-9d1e-c70c7d8e13c9)


Budgets & alerts, Observability and pub sub topics are in the end of this file.

# BRONZE


Ingest raw data from the Open Brewery DB API and store it in the Bronze layer (GCS).

Fetches brewery data from an API every hour, checking if the data has changed by comparing its hash to a stored one of the previous data.
If the data has changed, it stores the new data in GCS and updates the stored hash, and updates Big Query bronze table.
It also logs all the steps and sends email alerts in case of failures.

Paths:
bucket-case-abinbev/data/bronze/breweries_raw.json # file with data from endpoint
bucket-case-abinbev/data/bronze/last_update.txt # updates the stored hash

Table info:
case-abinbev.Medallion.bronze

Schema properties:

![image](https://github.com/user-attachments/assets/730bf92e-3431-4bd5-b48b-1fe95f5bbb47)

Query results:

![image](https://github.com/user-attachments/assets/01fe8811-fead-43d1-8de1-83db356ff2ec)



# SILVER


Process data from the Bronze layer, transform it, and store it in the Silver layer (BigQuery).

Waits for a signal from a Bronze layer DAG to indicate new data and downloads the new data from GCS.
Then, cleans and transforms the data using pandas, saving the parquet file in GCS, loading the transformed data into BigQuery.
Logs all steps and sends email alerts for failures.

Paths:
bucket-case-abinbev/data/silver/breweries_transformed/breweries_transformed.parquet # file with transformed data

Table info:
case-abinbev.Medallion.silver

Schema properties:

![image](https://github.com/user-attachments/assets/ebf4ac84-73ed-4b9b-9ce2-209f8661ebbc)

Partition:

![image](https://github.com/user-attachments/assets/89acfacd-a33d-4c3d-9791-6ca246b4b3b7)

Preview:

![image](https://github.com/user-attachments/assets/63bd9833-bf21-4af4-80cb-23cf337b01d2)



# GOLD

Aggregate data from the Silver layer and store it in the Gold layer (BigQuery).

Waits for the successful completion of a Silver DAG, retrieving transformed data from GCS.
Then, performs aggregations to create a Gold layer dataset, loading the Gold layer data into BigQuery for analysis and reporting.
Includes logging and email alerting for monitoring and debugging.

Paths:
bucket-case-abinbev/data/gold/breweries_aggregated.parquet #aggregated data 

Table info:
case-abinbev.Medallion.gold

Schema properties:

![image](https://github.com/user-attachments/assets/277b55e7-e28c-464b-b4ec-985fdffd039d)

Partition: 

![image](https://github.com/user-attachments/assets/7766a23c-5b2c-4270-923c-968f09932609)


# Creating and Configuring Google Cloud Composer Environment

This is a detailed explanation of how the Google Cloud Composer environment was created, configured, and used to manage the Airflow DAGs and resources.

# 1. Creating the Composer Environment
Initiating the Environment
A new Composer environment was created using the Google Cloud Console or the Cloud SDK.
Command (if done via SDK):

gcloud composer environments create composer-case \
  --location=us-central1 \
  --image-version=composer-3-airflow-2.9.3 \
  --node-count=3
Key settings:

Region: us-central1
Composer Version: composer-3 (Preview)
Airflow Version: 2.9.3
Node Count: 3 (for better scalability)
Setting Up the Environment
Once created, the environment was accessible through the Composer tab in Google Cloud Console. From here:

The DAGs folder URL was noted (a Google Cloud Storage bucket: us-central1-composer-case-e66c77cc-bucket).

# 2. Configuring the Google Cloud Storage Buckets
DAGs Folder
The Composer environment automatically created a bucket in Google Cloud Storage to store Airflow DAGs.
Example structure:


gs://us-central1-composer-case-e66c77cc-bucket/dags/
Additional Buckets
Separate buckets were created for raw data, transformed data, logs, and control files.
Commands:

gsutil mb -l us-central1 gs://bucket-case-abinbev/
gsutil mb -l us-central1 gs://us-central1-composer-case-e66c77cc-bucket/
Structure:

![image](https://github.com/user-attachments/assets/a7f49657-478b-4b13-a378-2700a3b4d4eb)


# 3. Setting Up DAGs
DAG Development
DAGs were developed locally. Each DAG was carefully crafted to adhere to Airflow standards. This included:

Importing required libraries (airflow, google-cloud-storage, pandas, etc.).
Configuring tasks (PythonOperator, Sensors, etc.).
Defining dependencies using >> and <<.
Uploading DAGs
DAG scripts were uploaded to the Composer bucket.
Command:

gsutil cp bronze.py silver.py gold.py gs://us-central1-composer-case-e66c77cc-bucket/dags/
After uploading, Airflow automatically detected the DAGs, and they became visible in the Airflow UI.

# 4. Installing Python Dependencies
Requirements File
Dependencies for all DAGs were listed in a requirements.txt file. Example:

google-cloud-storage
google-cloud-bigquery
pandas
requests
Installing Dependencies 

The file was uploaded to the Composer environment bucket:

gsutil cp requirements.txt gs://us-central1-composer-case-e66c77cc-bucket/config/requirements.txt
Dependencies were installed using:

gcloud composer environments update composer-case \
  --location=us-central1 \
  --update-pypi-packages-from-file=gs://us-central1-composer-case-e66c77cc-bucket/config/requirements.txt
  
# 5. Configuring Airflow Variables and Connections
Setting Up Variables
Variables for the DAGs were configured in the Airflow UI or via the Cloud SDK.

gcloud composer environments run composer-case \
  --location=us-central1 variables -- \
  -s bucket_name bucket-case-abinbev
  
Setting Up Connections
Airflow connections were set up using the Airflow UI (e.g., GCP connection for service accounts).

# 6. Testing the DAGs
Manual Triggering
Each DAG was triggered manually in the Airflow UI to ensure proper execution.
Logs were reviewed via the Airflow UI to confirm task success.

# Monitoring and Debugging


# 7. Setting Up Notifications
Email Alerts
The DAGs were configured to send email alerts in case of task failures.
Example snippet:

def alert_email_on_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url
    email = "your-email@example.com"
    subject = f"Failure Alert - DAG: {dag_id}, Task: {task_id}"
    body = f"""
    <h3>Failure in DAG</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></p>
    """
    send_email(to=email, subject=subject, html_content=body)
    
Google Cloud Monitoring

Alerting rules were configured for Composer health metrics to ensure proactive monitoring.

# 8. Maintaining and Iterating
Updating DAGs
DAG updates were done locally and re-uploaded to the Composer bucket:

gsutil cp updated_dag.py gs://us-central1-composer-case-e66c77cc-bucket/dags/


# PUB SUB TOPIC: Amount spent alert

Topics and subscriptions about spending and environment alerts:

![image](https://github.com/user-attachments/assets/9e29ab30-d8cb-4240-baf7-f8ab93490761)

# Budgets & alerts

![image](https://github.com/user-attachments/assets/e691be85-24af-4069-90d0-ced5ed6dc252)

# Observability 

![image](https://github.com/user-attachments/assets/67ddaeea-26dc-46c9-93d4-830885676029)


Boolean threshold

Healthy env: 1
If the env is not healthy, (below 1 is the threshold)

![image](https://github.com/user-attachments/assets/f02677c9-43f2-4d5b-a99d-1a5769c9f514)

If the policy reaches below 1, it will trigger the alert

![image](https://github.com/user-attachments/assets/22121660-b8e4-4c69-b9dc-809afd65b45d)


GCS: Google Cloud Storage
