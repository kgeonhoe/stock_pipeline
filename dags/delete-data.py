from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG("delete-old-data", 
          default_args=default_args, 
          max_active_runs=1, 
          schedule_interval="15 0 * * *", 
          catchup=False, 
          tags=['data', 'delete'])

dt = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
download_data = BashOperator(
    task_id='download-data',
    bash_command=f"/opt/airflow/jobs/delete-data.sh {dt} ",
    dag=dag
)
