from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def my_func():
   print('Hello from my_func')
   return 'hello from my_func'


def new_func(**kwargs):
   print(kwargs)
   return kwargs['param_1']

with DAG(
  "etl_sales_daily",
  start_date=days_ago(1),
  schedule_interval=None,
) as dag:
   task_a = DummyOperator(task_id="task_a")
   task_b = DummyOperator(task_id="task_b")
   task_c = DummyOperator(task_id="task_c")
   task_d = DummyOperator(task_id="task_d")
   python_task = PythonOperator(task_id='python_task', python_callable=my_func)
   python_task_1 = PythonOperator(task_id='python_task_1', python_callable=new_func,
                                op_kwargs={'param_1': 'one', 'param_2': 'two', 'param_3': 'three'})


   task_a >> [task_b, task_c]
   task_c >> python_task >> python_task_1 >> task_d