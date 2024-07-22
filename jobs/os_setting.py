


def airflow_file_path_setting() : 
    import os 
    
    def is_running_in_airflow(): 
        return "AIRFLOW_HOME" in os.environ
    
    def is_running_in_spark(): 
        return "SPARK_HOME" in os.environ
    
    if is_running_in_airflow() : 
        file_path = '/opt/bitnami/spark'
        print("This script is running within Apache Airflow.")     
    elif is_running_in_spark() : 
        file_path = '/opt/bitnami/spark'
        print("This script is running within Apache Airflow.")     
    else:
        file_path = '/root/project/de-2024'
        # Airflow에서 실행될 때 수행할 코드
        # 여기에 Airflow에서 실행될 코드 로직을 추가하세요.
        print("This script is running as a standalone Python script.")
    return file_path
