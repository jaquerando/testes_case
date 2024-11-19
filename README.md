# testes_case


This data pipeline ingests data from the Open Brewery DB API, performs transformations, and loads it into a data lake structured with Bronze, Silver, and Gold layers. 
It leverages Apache Airflow for orchestration, ensuring scheduled execution, error handling, and dependency management between the stages. 
Google Cloud Storage is used for data storage, and BigQuery serves as the final destination for analytical queries.

Medallion Architecture: Implements the Bronze, Silver, Gold layered approach for data lake organization.
Incremental Loading: Only processes new or changed data, improving efficiency.
Data Validation: Uses hash comparisons to ensure data integrity.
Monitoring and Alerting: Includes logging to GCS and email alerts for proactive monitoring.
Orchestration: Utilizes Apache Airflow for scheduling, task dependencies, and error handling.

BRONZE

Ingest raw data from the Open Brewery DB API and store it in the Bronze layer (GCS).

Fetches brewery data from an API every hour, checking if the data has changed by comparing its hash to a stored one of the previous data.
If the data has changed, it stores the new data in GCS and updates the stored hash, and updates Big Query bronze table.
It also logs all the steps and sends email alerts in case of failures.

Paths:
bucket-case-abinbev/data/bronze/breweries_raw.json # file with data from endpoint
bucket-case-abinbev/data/bronze/last_update.txt # updates the stored hash

Table info:
case-abinbev.Medallion.bronze


SILVER

Process data from the Bronze layer, transform it, and store it in the Silver layer (BigQuery).

Waits for a signal from a Bronze layer DAG to indicate new data and downloads the new data from GCS.
Then, cleans and transforms the data using pandas, saving the parquet file in GCS, loading the transformed data into BigQuery.
Logs all steps and sends email alerts for failures.

Paths:
bucket-case-abinbev/data/silver/breweries_transformed/breweries_transformed.parquet # file with transformed data

Table info:
case-abinbev.Medallion.silver

GOLD

Aggregate data from the Silver layer and store it in the Gold layer (BigQuery).

Waits for the successful completion of a Silver DAG, retrieving transformed data from GCS.
Then, performs aggregations to create a Gold layer dataset, loading the Gold layer data into BigQuery for analysis and reporting.
Includes logging and email alerting for monitoring and debugging.

Paths:
bucket-case-abinbev/data/gold/breweries_aggregated.parquet #aggregated data 

Table info:
case-abinbev.Medallion.gold




GCS: Google Cloud Storage
