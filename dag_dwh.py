from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# List of tables to process
tables = ['customers', 'employees', 'shippers', 'products', 'orders', 'order_details']

def extract_data(table_name):
    # Function to extract data from PostgreSQL for a specific table
    return f'SELECT * FROM {table_name}'

def transform_data():
    # Function for data transformation
    pass

with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG to move data from PostgreSQL to BigQuery',
    schedule_interval='0 0 * * *',  # Runs daily at midnight UTC
    start_date=datetime(2024, 6, 25),
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Iterate over tables and create tasks dynamically
    for table in tables:
        extract_task = PostgresOperator(
            task_id=f'extract_data_{table}',
            postgres_conn_id='postgres_conn',
            sql=extract_data(table),  # Use function to dynamically set SQL query
        )

        load_to_gcs_task = PostgresToGCSOperator(
            task_id=f'load_{table}_to_gcs',
            postgres_conn_id='postgres_conn',
            sql=extract_data(table),  # Use function to dynamically set SQL query
            bucket='gcs_bucket',
            filename=f'data/{table}_{{{{ ds }}}}.json',
            export_format='json',
        )

        load_to_bq_task = GCSToBigQueryOperator(
            task_id=f'load_{table}_to_bq',
            bucket='gcs_bucket',
            source_objects=[f'data/{table}_{{{{ ds }}}}.json'],
            destination_project_dataset_table=f'project3.dwh.{table}',
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition='WRITE_TRUNCATE',
        )

        # Set task dependencies
        extract_task >> load_to_gcs_task >> load_to_bq_task
        extract_task >> transform_task

    transform_task >> load_to_gcs_task  # Ensure transformation completes before loading to GCS
    load_to_gcs_task >> load_to_bq_task  # Ensure data is loaded to GCS before loading to BigQuery
