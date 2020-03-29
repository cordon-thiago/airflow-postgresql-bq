"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
# Add path where are the additional modules for ETL 
import sys
sys.path.append('./modules/etl/')

# Import ETL modules
from etl import Etl

###############################################
# Variables
###############################################
raw_table = "public.hardbounce_raw"
stg_table = "public.hardbounce_stg"
csv_source_file = "/usr/local/datasets/hardbounce.csv"
file_delimiter = ";"
pg_str_conn = "dbname='test' user='test' host='postgres' password='postgres'"

###############################################
# Functions
###############################################
def import_file(csv_source_file, file_delimiter, pg_str_conn, raw_table):
    etl_test = Etl()
    etl_test.pg_load_from_csv_file(
            csv_source_file=csv_source_file, 
            file_delimiter=file_delimiter,
            pg_str_conn=pg_str_conn, 
            pg_schema=raw_table.split(".")[0], 
            pg_dest_table=raw_table.split(".")[1]
        )

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
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

dag = DAG(
        "test", 
        default_args=default_args, 
        schedule_interval=timedelta(1), 
        template_searchpath="./sql/etl/"
    )

start = DummyOperator(task_id="start", dag=dag)

create_raw_table = PostgresOperator(
    task_id="create_raw_table", 
    postgres_conn_id="postgres_default",
    sql="hardbounde_raw_ddl.sql",
    params={"raw_table":raw_table},
    dag=dag)

import_file = PythonOperator(
    task_id="import_file", 
    python_callable=import_file,
    op_kwargs={"csv_source_file":csv_source_file, "file_delimiter":file_delimiter, "pg_str_conn":pg_str_conn, "raw_table":raw_table},
    dag=dag)

create_stg_table = PostgresOperator(
    task_id="create_stg_table", 
    postgres_conn_id="postgres_default",
    sql="hardbounce_stg.sql",
    params={"stg_table":stg_table, "raw_table":raw_table},
    dag=dag)

start >> create_raw_table >> import_file >> create_stg_table