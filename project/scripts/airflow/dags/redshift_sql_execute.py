from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator




with DAG(
  "redshiftload_data",
  start_date=days_ago(1),
  schedule_interval=None,
) as dag:
   task_a = DummyOperator(task_id="task_a")
   task_b = DummyOperator(task_id="task_b")
   task_c = DummyOperator(task_id="task_c")
   task_d = DummyOperator(task_id="task_d")

   redshifttask = RedshiftSQLOperator(
        task_id='redshifttask',
        redshift_conn_id='quintrix-redshift',
        sql="select cust_id,stat_cd,tran_date,tran_ammt from cards_ingest.tran_fact",
    )

   task_a >> [task_b, task_c]
   task_c >> redshifttask
