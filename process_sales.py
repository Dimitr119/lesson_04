from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

RAW_DIR = os.path.join("/raw", "sales", "2022-08-09")
STG_DIR = os.path.join("/stg", "sales", "2022-08-09")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    dag_id = "process_sales",
    default_args = default_args,
    description = 'lesson_04'
)

extract_data_task = BashOperator(
    task_id = 'extract_data_from_api',
    bash_command='curl -X POST -H "Content-Type: application/json" -d \'{"date": "2022-08-09", "raw_dir": "/raw/sales/2022-08-09"}\' http://host.docker.internal:8081/',
    dag=dag,
)


convert_data_task = BashOperator(
    task_id = 'convert_to_avro',
    bash_command='curl -X POST -H "Content-Type: application/json" -d \'{"raw_dir":"/raw/sales/2022-08-09", "stg_dir": "/raw/sales/2022-08-09"}\' http://host.docker.internal:8082/',
    dag=dag,
)

extract_data_task >> convert_data_task