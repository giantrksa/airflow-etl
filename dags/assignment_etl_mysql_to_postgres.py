import logging
import os
import json
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

def load_config():
    """
    Loads the ETL configuration from the etl_config.json file located in the resources folder.
    
    :return: Dictionary containing configuration parameters.
    """
    dag_folder = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dag_folder, 'resources', 'etl_config.json')
    
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            logging.info(f"Configuration loaded successfully from {config_path}")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing the configuration file: {e}")
        raise

CONFIG = load_config()

def etl_task(table_name, mysql_conn_id, postgres_conn_id, target_schema, **kwargs):
    """
    ETL task to extract data from MySQL and load it into PostgreSQL.
    
    :param table_name: Name of the table to process.
    :param mysql_conn_id: Airflow connection ID for MySQL.
    :param postgres_conn_id: Airflow connection ID for PostgreSQL.
    :param target_schema: Target schema in PostgreSQL.
    """
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    logging.info(f"Starting ETL for table: {table_name}")
    
    try:
        df = mysql_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}")
        logging.info(f"Extracted {len(df)} records from MySQL table '{table_name}'")
    except Exception as e:
        logging.error(f"Failed to extract data from MySQL table '{table_name}': {e}")
        raise
    
    if df.empty:
        logging.warning(f"No data found in MySQL table '{table_name}'. Skipping load.")
        return
    


    target_table = f"{target_schema}.{table_name}"
    
    try:
        postgres_hook.insert_rows(
            table=target_table,
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
            replace=True 
        )
        logging.info(f"Successfully inserted {len(df)} records into PostgreSQL table '{target_table}'")
    except Exception as e:
        logging.error(f"Error inserting data into PostgreSQL table '{target_table}': {e}")
        raise

def check_week(**kwargs):
    """
    Ensures that the ETL runs only on the first and third Fridays of the month.
    
    :param kwargs: Airflow context parameters.
    """
    execution_date = kwargs['execution_date']

    week_of_month = (execution_date.day - 1) // 7 + 1
    logging.info(f"Execution date: {execution_date}, Week of month: {week_of_month}")
    
    if week_of_month not in [1, 3]:
        logging.info("Not the first or third week of the month. Skipping ETL tasks.")
        raise ValueError("Not the first or third week of the month.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [CONFIG['email']['on_failure']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': CONFIG['retry_policy']['retries'],
    'retry_delay': timedelta(minutes=CONFIG['retry_policy']['retry_delay_minutes']),
}

with DAG(
    dag_id='assignment_etl_mysql_to_postgres',
    default_args=default_args,
    description='Dynamic DAG for ETL from MySQL to PostgreSQL for 5 tables',
    schedule_interval='15 9-21/2 * * 5',
    start_date=days_ago(1),
    catchup=False,
    tags=['mysql', 'postgres', 'etl', 'dynamic', 'weekly'],
) as dag:
    
    check_week_task = PythonOperator(
        task_id='check_week',
        python_callable=check_week,
        provide_context=True,  
    )
    
    for table in CONFIG['tables']:
        etl = PythonOperator(
            task_id=f'etl_{table}',
            python_callable=etl_task,
            op_kwargs={
                'table_name': table,
                'mysql_conn_id': CONFIG['mysql_conn_id'],
                'postgres_conn_id': CONFIG['postgres_conn_id'],
                'target_schema': CONFIG['target_schema']
            },
            provide_context=True,
        )
        check_week_task >> etl
