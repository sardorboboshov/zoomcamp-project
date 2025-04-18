# Santander bicycle rentals analytics

## Problem description
This project is focused on processing data and building Data Warehouse in Google BigQuery from the dataset Santander bicycle rentals in London. 

Airflow is used to fetch and process raw data from dataset and upload the files to gcp bucket and create the tables in staging phase.

DBT is used to process the uploaded data and create the DWH in BigQuery from uploaded data.

Airflow will trigger the DBT Cloud Job every month.
## Data Pipeline
Current project uses Batch Streaming and Airflow used for orchestration.

## Transformations
Transformations happens 2 times in the project:
1. **Polars, Pandas**: Columns have inconsistent number of columns and names, so they neeed to be in 1 schema so table can be created from them without any problem
2. **DBT Cloud**: DBT Cloud takes the data from staging tables and creates the table. Cloud job will build the models and push the created tables in prod dataset.

## Technologies
1. **Cloud**: GCP
2. **Workflow orchestration**: Airflow
3. **Data Warehouse**: BigQuery
4. **Batch processing**: Polars, Pandas

## Dashboard
For visulisation, Looker Studio is used - here is the [Link](https://lookerstudio.google.com/reporting/45ebd5d9-2890-4b59-b6ec-0a33117ad953)

Dashboard has 2 graphs:
1. **First graph shows the monthly count of bike rental accross the 12 months**
2. **Second graph shows the distribution of bike rentals among directions**

Visualisation created for choosen year(s) and direction(s).

![graphs](dashboard.png)   

## Reproducibility
Prerequisites:
1. You need docker installed
2. GCP account
3. DBT account

The steps to reproduce the project:

1. Go to the airflow folder:
    
    1. Run **docker-compose build .** to build the docker image
    2. Run **docker-compose up -d** to start the containers

2. Create the project in GCP. 

3. Create the DBT project - in order to link your gcp and github follow the steps [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md), dont forget to save your service_account

4. Add the Google and DBT credentials to Variables and Connections

5. Required Variables for Airflow:  
    1. **BUCKET_NAME** - the bucket name in your GCP, this is where all of your data will be stored
    2. **DATA_LAKE_DESTINATION_PATH** - the folder name inside of BUCKET to store the files
    3. **DATASET_PATH** - the dataset path inside of airflow to store the downloaded files from the website. We need to store these files in order to process and upload them to GCP in later stages.
    4. **DBT_JOB_ID** - the job id for pushing the data to prod dataset

6. Required Connectons for Airflow:
    1. **dbt_cloud** - this connection is used to trigger the DBT job. Connection type must be **dbt Cloud**. **account_id** and **API Token** fields must be filled
    2. **google_cloud** - this connection is used to upload the files and create the tables. 
    You need to fill the project_id and put the json file to **Keyfile JSON** field