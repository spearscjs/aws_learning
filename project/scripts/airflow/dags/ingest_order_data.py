from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
import logging


with DAG(
  "ingest_order_data",
  start_date=days_ago(1),
  schedule_interval=None,
) as dag:
   
    s3_file_chk= S3KeySensor(
        task_id='s3_file_chk',
        bucket_key='s3://quintrix-spearscjs/raw/cards_ingest/order_data/order_data_20230201.csv'
    )

''' 
1. Create a dag to have 4 task


Task 1. sensor
use date parameter from dag.
date=20230201
1. Check file exists in s3 bucket1/folder1/order_data_{date}.csv

Task 2:
2. Copy the file to another folder with
bucket1/folder2/2023-02-01/order_data_20230201.csv
bucket1/folder2/2023-02-02/order_data_20230202.csv
'''
 


'''
Task3:
3. Run the crawler to add the partition as dataset_date

Task4 .
Run a glue job (spark job)
get the total ammount by brand_name,product_name,dataset_date and load into file. Makse sure you have dynamic partition by brand_name
'''


