# add scripts folder to PYTHONPATH
import sys
sys.path.insert(1, '/home/pedro/Projects/glass-dome/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from AR_crawler import start_ar_crawler

default_args = {
    'owner': 'Pedro Almeida',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email': 'pedro.gd.almeida@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'glassdome_pt_ar_dag', # glassdome DAG for Portugal Assembleia da Republica 
    default_args = default_args,
    description = 'Glassdome DAG for the Portuguese parliament, or Assembleia da Republica'
)

ar_crawler = PythonOperator(
    task_id = 'ar_crawler',
    python_callable = start_ar_crawler,
    dag = dag
)

# ar_downloader = PythonOperator(
#     task_id = 'ar_downloader',
#     python_callable = start_ar_crawler,
#     dag = dag
# )

ar_crawler #>> ar_downloader