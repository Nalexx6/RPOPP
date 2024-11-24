import os
import sys

# It is required to add custom python module location into path before importing module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from lib.ingestion_pipeline import IngestionPipeline

# templated fields
dataset_id = 'test_dag'
dataset_path = 'datasetPath'
tags = ['tags']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'end_date': None,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(dag_id=dataset_id,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=False,
         tags=tags) as dag:
    IngestionPipeline(dataset_id, dataset_path).build()
