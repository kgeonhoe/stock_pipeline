import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

 # 각각의 별표(*)는 다음과 같은 의미를 가집니다.
 # 분(Minute) : 0부터 59까지의 값을 가집니다.
 # 시간(Hour) : 0부터 23까지의 값을 가집니다.
 # 일(Day of the month) : 1부터 31까지의 값을 가집니다.
 # 월(Month) : 1부터 12까지의 값을 가집니다.
 # 요일(Day of the week) : 0부터 7까지의 값을 가집니다. (0과 7은 일요일)
 
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    # "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # "on_failure_callback": ,
}
 
dag = DAG("for_merge", 
          default_args=default_args, 
          max_active_runs=1, 
          schedule_interval="31 15 * * *", 
          catchup=False, 
          tags=['data'])

start = PythonOperator(
    task_id="data_collect",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

# Python 파일 실행 태스크
python_job = BashOperator(
    task_id="run_python_script",
    bash_command="python /opt/airflow/jobs/for_merge.py",  # 여기에 실제 Python 파일 경로를 넣으세요
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> end 

# dag = DAG("download-github-archive", 
#           default_args=default_args, 
#           max_active_runs=1, 
#           schedule_interval="0 * * * *", 
#           catchup=False, 
#           tags=['data'])

# dt = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# download_data = BashOperator(
#     task_id='download-data',
#     bash_command=f"/opt/airflow/jobs/spark-submit.sh {dt} ",
#     dag=dag
# )