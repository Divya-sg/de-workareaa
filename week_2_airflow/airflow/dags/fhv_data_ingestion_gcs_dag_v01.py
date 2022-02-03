import os
import logging

from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

base_dataset_file = "fhv_tripdata_"
base_dateset_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
#parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(**context):
    #if not src_file.endswith('.csv'):
    #    logging.error("Can only accept source files in CSV format, for the moment")
    #    return
    data_interval_start = context['data_interval_start'].strftime('%Y-%m')
    src_file = f"{path_to_local_home}/{base_dataset_file+data_interval_start+'.csv'}" 
    
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, **context):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    data_interval_start = context['data_interval_start'].strftime('%Y-%m')
    object_name = f"parquet/{base_dataset_file+data_interval_start+'.parquet'}"
    local_file = f"{path_to_local_home}/{base_dataset_file+data_interval_start+'.parquet'}" 
    
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "start_date": datetime(2019,1,1),
    "end_date" : datetime(2020,12,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fhv_data_ingestion_gcs_dag_v01",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=3, 
    tags=['dtc-de'],
) as dag:

    
    commands = """ 
    date={{dag_run.data_interval_start.strftime('%Y-%m')}}
    base_dataset_url="https://s3.amazonaws.com/nyc-tlc/trip+data/"
    base_dataset_file="fhv_tripdata_"
    path_to_local_home="/opt/airflow/"
    echo $base_dataset_url$base_dataset_file$date'.csv'
    curl -sSLf $base_dataset_url$base_dataset_file$date'.csv' > $path_to_local_home$base_dataset_file$date'.csv'
    
    """
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=commands,
        dag=dag,
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        #op_kwargs={
        #    "src_file": f"{path_to_local_home}/{base_dataset_file+{{dag_run.data_interval_start.strftime('%Y-%m')}}+'.csv'}",
        #},
        dag=dag,
        provide_context=True,
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
        #    "object_name": f"parquet/{base_dataset_file+{{dag_run.data_interval_start.strftime('%Y-%m')}}+'.parquet'}",
        #    "local_file": f"{path_to_local_home}/{base_dataset_file+{{dag_run.data_interval_start.strftime('%Y-%m')}}+'.parquet'}",
        },
        dag=dag,
        provide_context=True,
    )

    commands_remove = """ 
    date={{dag_run.data_interval_start.strftime('%Y-%m')}}
    base_dataset_url="https://s3.amazonaws.com/nyc-tlc/trip+data/"
    base_dataset_file="fhv_tripdata_"
    path_to_local_home="/opt/airflow/"
    rm $path_to_local_home$base_dataset_file$date'.csv' $path_to_local_home$base_dataset_file$date'.parquet'
    
    """

    remove_dataset_task = BashOperator(
        task_id="remove_dataset_task",
        bash_command=commands_remove,
        dag=dag,
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task
    
