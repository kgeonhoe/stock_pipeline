#%%
from stock_filter import StockFilter
from collector import * 
import cf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
import pyarrow as pa 
from pathlib import Path
import argparse
import os 
import datetime 
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pymysql 

def is_running_in_airflow(): 
    return "AIRFLOW_HOME" in os.environ

if __name__ == "__main__":
    if is_running_in_airflow(): 
        file_path = '/opt/bitnami/spark'
        print("This script is running within Apache Airflow.")
        # Airflow에서 실행될 때 수행할 코드
        # 여기에 Airflow에서 실행될 코드 로직을 추가하세요.
    else:
        file_path = '/root/project/de-2024'
        print("This script is running as a standalone Python script.")
    
    collector = Collector(   appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)
    
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database=cf.database, use_unicode=True, charset='utf8')
    cursor = conn.cursor()
   
    spark =  (SparkSession
             .builder
             .master("local")
             .appName("es-test")
             .config("spark.driver.extraClassPath", file_path + "/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .config("spark.jars", file_path + "/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .getOrCreate())
   
    parser = argparse.ArgumentParser() 
    args = parser.parse_args()
    
    args.spark = spark  
    args.conn = conn 
    args.cursor = cursor
    args.file_path = file_path
    filter = StockFilter(args)
    table_list = filter.get_all_stock_mysql('airflow_daily_craw')
    
    for tbl in table_list : 
        filter.drop_table_mysql(tbl)
    