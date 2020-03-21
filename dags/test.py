"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
# Add path where are the additional modules for ETL 
import sys
sys.path.append('./modules/etl/')

# Import ETL modules
from etl import Etl


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 3, 21),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def check_table_exists():
    etl_test = Etl()
    print(etl_test.pg_check_table_exists("postgres_test","public","dataset_test"))

dag = DAG("test", default_args=default_args, schedule_interval=timedelta(1))

start = DummyOperator(task_id="start", dag=dag)

test = PythonOperator(task_id="test", python_callable=check_table_exists, dag=dag)

start >> test