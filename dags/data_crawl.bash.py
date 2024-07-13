from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# 기본 설정 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'spark_submit_11dag',
    default_args=default_args,
    description='A simple DAG to run spark-submit.sh',
    schedule_interval=timedelta(days=1),
)

# BashOperator를 사용하여 스크립트 실행
run_spark_submit = BashOperator(
    task_id='run_spark_submit',
    bash_command='bash /opt/bitnami/spark/jobs/spark-submit.sh /opt/bitnami/spark/jobs/main.py',
    dag=dag,
)

run_spark_submit
