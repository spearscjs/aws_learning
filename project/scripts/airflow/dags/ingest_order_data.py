
# USAGE: airflow tasks test ingest_order_data cp_files -t '{"dates" : "2023-02-04,2023-02-05,2023-02-06,2023-02-07,2023-02-08,2023-02-09,2023-02-10"  }'


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



from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.models.param import Param




def chk_dates(**kwargs):
    dates = kwargs['params']['dates'].split(',')
    paths = []

    # generate file names
    for d in dates:
        d_str = d.replace('-','') #file names do not include dashes in s3
        paths.append(f's3://quintrix-spearscjs/raw/cards_ingest/order_data/order_data_{d_str}.csv')
   
    s3_file_chk= S3KeySensor(
      task_id='s3_file_chk',
      bucket_key=paths
    )

    s3_file_chk.execute(dict())


def cp_files(**kwargs):
    dates = kwargs['params']['dates'].split(',')
    for d in dates:
        d_str = d.replace('-','') #file names do not include dashes in s3
        src = f's3://quintrix-spearscjs/raw/cards_ingest/order_data/order_data_{d_str}.csv'
        dest = f's3://quintrix-spearscjs/data/cards_ingest/order_data/dataset_date={d}/order_data_{d_str}.csv'
        copyOperator = S3CopyObjectOperator(
          task_id='cp_file',
          source_bucket_key=src,
          dest_bucket_key=dest
        )
        copyOperator.execute(dict())

def start_glue_crawler():
    # client.start_crawler(Name='order_data_crawler')
    print("ASSASD")
    

with DAG(
  "ingest_order_data",
  start_date=days_ago(1),
  schedule_interval=None,
) as dag:
    chk_dates_op = PythonOperator(task_id='chk_dates', python_callable=chk_dates)
    cp_files_op = PythonOperator(task_id='cp_files', python_callable=cp_files)
    glue_crawler_op =  GlueCrawlerOperator(
        task_id = 'glue_crawler',
        config={"Name" : "order_data_crawler"},
        dag=dag,
        region_name = 'us-east-1',
        wait_for_completion=True
        
    )
    glue_job_op = GlueJobOperator(
        task_id = 'glue_job',
        job_name='order_data_brand_totals',
        dag=dag,
        region_name = 'us-east-1',
        s3_bucket = 's3://aws-glue-assets-775856445594-us-east-1',
        iam_role_name='AWSGlueServiceRole-1',
        script_location='s3://aws-glue-assets-775856445594-us-east-1/scripts/order_data_brand_totals.py'
    )


    chk_dates_op >> cp_files_op >> glue_crawler_op >> glue_job_op

    

    
    




