import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # "on_failure_callback": ,
}
 
dag = DAG("download-stock-data", 
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
crawl_main = BashOperator(
    task_id="run_python_script",
    bash_command="python /opt/airflow/jobs/stock_crawl_main.py",  # 여기에 실제 Python 파일 경로를 넣는다
    dag=dag
)

filter_data = BashOperator( 
            task_id = 'filter-data', 
            bash_command = "/opt/airflow/jobs/spark-submit.sh /opt/airflow/jobs/stock_add_colume.py",
            dag=dag
   )

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> crawl_main >> end 

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