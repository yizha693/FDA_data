import sys

code_dir = "/home/airflow/gcs/dags/"
sys.path.append(code_dir)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from config.config import Config
from tasks.tasks import GetReports
from utils.utils import Utils


cfg = Config()
parse_the_FEARS = GetReports().parse_the_FEARS
orange_book_retrieive = GetReports().orange_book_retrieive
df_to_table = Utils().df_to_table

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 19),
    "email": cfg.AIRFLOW['dag_email'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def generate_orange_book_data ():
    orange_book_retrieive()
    return

def update_fears_task ():
    df_to_table(
        df = parse_the_FEARS(
            orange_book_retrieive
        ),
        destination_dataset='fda',
        destination_table='fears',
        write_method="APPEND"

    )
    return 

with DAG(
        'FEARS',
        default_args=default_args,
        schedule_interval='0 0 1 * *'
        ) as dag:
    
    task_1 = PythonOperator(
        task_id="update_the_fears_table",
        python_callable=update_fears_task,
        dag=dag,
    )
    task_2 = PythonOperator(
        task_id="generate_orange_book_data",
        python_callable=generate_orange_book_data,
        dag=dag,
    )
task_1 >> task_2
