from airflow import DAG 
import datetime
import pendulum ## datetime을 더 쉽게 쓸수있게 만들어줌. 
from airflow.operators.bash import BashOperator


with DAG(dag_id="dags_bash_operator", 
        #  schedule = "31 15 * * *", 
         start_date = pendulum.datetime(2024,7,9, tz = "Asia/Seoul"), 
         catchup=False, # True 로 놓을 경우 start_date 부터 현재까지의 누락된 데이터에 대해 한꺼번에 동작을 수행 함. 일반적으로  False 로 놓는다. 
        # dagrun_timeout = datetime.timedelta(minutes=60),  # 얼마만큼의 시간이 지난뒤에 timeout 이 나오는지 만드는 함수 
        # params = {"example_key": "example_value"},  ## task 들에 공통적으로 줄 파라미터 값들
        #   default_args=default_args, 
          max_active_runs=1, 
          schedule_interval="31 15 * * *", 
        tags=['data']  ## optional 한 기능 
        ) as dag : 
        bash_t1 = BashOperator( 
                               task_id = "bash_t1", 
                               bash_command="echo whoami", 
                               )
         
        bash_t2 = BashOperator( 
                                task_id = 'bash_t2', 
                                bash_command = "echo $HOSTNAME"
                                )
        
        bash_t1 >> bash_t2