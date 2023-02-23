from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.sensors.s3_key_sensor import S3KeySensor


default_args = {
    'owner': 'me',
    'start_date': datetime(2023, 2, 21),
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='glue_af_pipeline', default_args=default_args, schedule_interval=None) as dag:

    ''' 
    might need wildcard for 
    s3_cust_file_chk = S3KeySensor(
        task_id='s3_cust_file_chk',
        bucket_key='s3://quintrix-spearscjs/data/src_customer/customer_details_parquet/000000_0',
        bucket_name='quintrix-spearscjs',
        timeout=18 * 60 * 60,
        poke_interval=60 
    )
    '''
    task_a = DummyOperator(task_id="task_a")
    glue_cust_job = GlueJobOperator(
        task_id="glue_cust_job",
        job_name='test_glue_job',
    )

    task_a >> glue_cust_job