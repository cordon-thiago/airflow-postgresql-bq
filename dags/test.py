from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
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
bq_bucket = "bq-dataengineer-stg"


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

export_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="export_gcs",
    sql="export_pg_table.sql",
    params={"table":stg_table},
    bucket=bq_bucket,
    filename=stg_table,
    schema_filename=stg_table + "_schema",
    postgres_conn_id="postgres_default",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dag)

gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="gcs_to_bq",
    bucket=bq_bucket,
    source_objects=[stg_table],
    source_format="NEWLINE_DELIMITED_JSON",
    destination_project_dataset_table="bq-dataengineer:stg.{}".format(stg_table.split(".")[1]),
    autodetect=False,
    schema_object=stg_table + "_schema",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    bigquery_conn_id="bigquery_default",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dag)

start >> create_raw_table >> import_file >> create_stg_table >> export_gcs >> gcs_to_bq