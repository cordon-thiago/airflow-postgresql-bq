"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def test():
    etl_test = Etl()
    etl_test.pg_load_from_csv_file(
            csv_source_file="/usr/local/datasets/hardbounce.csv", 
            file_delimiter=";",
            pg_str_conn="dbname='test' user='test' host='postgres' password='postgres'", 
            pg_schema="public", 
            pg_dest_table="hardbounce_raw"
    )

dag = DAG("test", default_args=default_args, schedule_interval=timedelta(1), template_searchpath="./sql/etl/")

start = DummyOperator(task_id="start", dag=dag)

import_file = PythonOperator(
    task_id="import_file", 
    python_callable=test, 
    dag=dag)

create_stg_table = PostgresOperator(
    task_id="create_stg_table", 
    postgres_conn_id="postgres_default",
    sql="hardbounce_stg.sql",
    params={"stg_table":"public.hardbounce_stg", "raw_table":"public.hardbounce_raw"},
    dag=dag)

start >> import_file >> create_stg_table