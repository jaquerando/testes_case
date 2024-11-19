# Data pipeline summary

This data pipeline ingests data from the Open Brewery DB API, performs transformations, and loads it into a data lake structured with Bronze, Silver, and Gold layers. 
It leverages Apache Airflow for orchestration, ensuring scheduled execution, error handling, and dependency management between the stages. 
Google Cloud Storage is used for data storage, and BigQuery serves as the final destination for analytical queries.

Medallion Architecture: Implements the Bronze, Silver, Gold layered approach for data lake organization.
Incremental Loading: Only processes new or changed data, improving efficiency.
Data Validation: Uses hash comparisons to ensure data integrity.
Monitoring and Alerting: Includes logging to GCS and email alerts for proactive monitoring.
Orchestration: Utilizes Apache Airflow for scheduling, task dependencies, and error handling. 

# For detailed DOCKER deployment, please go to folder "DOCKER"

# Total cost of pipeline = R$ 471,35

Up to Nov 19th, total spent was around R$ 471,35, with Cloud Composer (R$ 467,53) and Cloud Storage R$ 3,82.

![image](https://github.com/user-attachments/assets/52dd34ec-2be2-442a-aebe-244c92ec6893)

![image](https://github.com/user-attachments/assets/e741493b-6617-40f9-9d1e-c70c7d8e13c9)


Budgets & alerts, Observability and pub sub topics are in the end of this file.

# Big Query Interface:

![image](https://github.com/user-attachments/assets/cf30ebf1-388e-4ebc-918c-daeb0ee647ca)

Datasets and tables

# LAYERS OF MEDALLION ARCHITECTURE

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

Partitioning Details for the Silver Table:

Table Type: Partitioned
Partitioned by: Integer Range
Partitioned on Field: state_partition
Partition Range Start: 0
Partition Range End: 50
Partition Range Interval: 1
Clustered by: country, city
Reasoning Behind This Design:
Efficient Query Performance:

The state_partition field was chosen for partitioning because it distributes data into manageable subsets. With an integer range of 0-50, it aligns with the possible states in the dataset, ensuring that each partition holds data for a single "hashed" state. This reduces query costs and improves scan efficiency for state-based queries.
It was necessary to create a new field to use partitioning, since Big Query normally allows only date fieds to be partitioned by default, so was necessary bypass this limitation.

Controlled Partition Range:
The range from 0 to 50 with an interval of 1 ensures a fixed number of partitions, which aligns with the expected number of U.S. states. This avoids creating excessive partitions and ensures a well-balanced dataset.

Clustering for Faster Filtering and Sorting:

Clustering by country and city allows queries to quickly filter and sort data based on these fields. This clustering is particularly helpful when performing city-level or country-level aggregations, reducing I/O operations.
Logical Data Organization:

Partitioning by a hashed state ensures even data distribution, while clustering by country and city organizes related rows in the same blocks for efficient retrieval.

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

Clustering Details for the Gold Table:

Clustered by: country, state, brewery_type
Reasoning Behind This Design:
Optimized for Analytical Queries:

The Gold table represents aggregated data. Clustering by country, state, and brewery_type aligns with common analytical queries that analyze breweries at a regional or categorical level. For example, users might filter by country and brewery type to see specific trends.
Reduced Query Latency:

Clustering organizes data by these fields, reducing the amount of data scanned when filtering on these dimensions. This directly improves query speed and reduces costs in BigQuery.
Logical Aggregation:

Aggregated data is often queried hierarchically, e.g., by country, then state, and then by brewery_type. Clustering ensures that related rows are physically stored close to each other, facilitating such queries.
No Partitioning:

Since the Gold table is smaller and represents aggregated data, partitioning wasn’t necessary. Clustering alone is sufficient to optimize queries.

Analysts can directly query the Gold layer without worrying about raw data inconsistencies or transformation logic.

# Leveraging the Processing in Google Cloud

Choosing the ToolKit

Integration with Google BigQuery: Efficient query optimization through partitioning and clustering - Serverless, fully managed environment scales seamlessly with data size.
Orchestration with Google Composer (Airflow):DAGs orchestrate processing steps between layers, ensuring reliability and dependency management.
Cloud Storage for Durability:Data persists across layers in buckets, allowing incremental updates and cost-effective long-term storage.
Serverless Execution via Cloud Run: Containerized DAGs provide isolated, scalable execution environments for each layer, ensuring consistent processing.

# LOGS

# BRONZE 
For the bronze layer, the dag is triggered every 1 hour, as seen in the logs bucket:

![image](https://github.com/user-attachments/assets/1a95aeac-90d0-4f89-a59c-b0830970a7ba)

It stores the last hash of the last execution. Comparing the old and the new one, if the new is equal to the previous execution, the log will return that there is no modification and will not proceed to update bronze data.

![image](https://github.com/user-attachments/assets/eb78fccb-4c53-42d5-806c-de7f78ce86df)

As we can see, the hashs are equal, so the file has not been updated, the silver dag will not be fired, nor the gold dag, even with the bronze dag triggering every hour, as we can see here:

![image](https://github.com/user-attachments/assets/f5020cec-7cb1-4681-b6dd-0dd2736683f1)

This is the direct log of bronze dag

![image](https://github.com/user-attachments/assets/d57ab0cc-12b7-4965-9de7-bd40c1eb3088)

The XCom return value
![image](https://github.com/user-attachments/assets/24327f12-d085-46bd-ace1-d2b487997189)

# SILVER
For the silver layer, it is only triggered when is found a difference between the new and old hash. 
Since until now this difference was not found, when triggered manually, the first task gets error because it is waiting for bronze signal comunicating the change of hash, that is not there, it is not happening (expected behaviour). It will only happen when there is a difference between old and new hash of bronze dag. 

![image](https://github.com/user-attachments/assets/6f106a9b-80cf-42f8-89ad-aa291e7a9c2f)

When there is difference between hashs or it is the first time executing (it does not have the last hash), the logs in the bucket will show up like this:

![image](https://github.com/user-attachments/assets/9e53593e-0096-448b-a7c8-5988bff13baf)


# GOLD

For the gold layer, it is only triggered when dag silver ends with success state. 
![image](https://github.com/user-attachments/assets/92a96aea-b324-450f-af2d-8abe76973e1a)

As the silver dag was not triggered, since the hashs are equal, gold dag will not execute, or will get error if we force it to be triggered manually (this case):

![image](https://github.com/user-attachments/assets/74e5c92c-4748-4b65-89b7-6f9577ff6c92)

When there is difference between hashs or it is the first time executing (it does not have the last hash), the logs in the bucket will show up like this:

![image](https://github.com/user-attachments/assets/f4afe673-75db-47bf-846b-8e8246ee1156)


# Creating and Configuring Google Cloud Composer Environment

# AIRFLOW DAGS

![image](https://github.com/user-attachments/assets/e9892e93-31eb-4a34-a495-b2ded98e93ed)

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
Dependencies for all DAGs were listed in a requirements.txt file.

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

Composer

![image](https://github.com/user-attachments/assets/03e731df-8c23-4c9b-ba5c-720d5e422561)

  
# 5. Configuring Airflow Variables and Connections
Setting Up Variables
Variables for the DAGs were configured in the Airflow UI and via the Cloud SDK.

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

Snippet:

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



# Glossary

GCS: Google Cloud Storage
