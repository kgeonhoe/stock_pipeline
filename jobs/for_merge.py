#%%
from stock_filter import StockFilter
from collector import *  ## 콜렉터 
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
import os_setting

#### 지금까지 잘못저장한 파일들을 한폴더에 넣기위함. 

if __name__ == "__main__":
    
    ## NOTE data 파일경로 잡아주는 목적 
    file_path = os_setting.airflow_file_path_setting()
    collector = Collector(   appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
    cursor = conn.cursor()
   
    ## NOTE SPARK 객체 생성 
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
    merge_list = filter.get_all_stock_mysql('airflow_daily_craw')
    if 'all_stock' in merge_list : 
        merge_list.remove('all_stock')
    
    try : 
        # with self.cursor as cursor : 
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS all_stock (
             stockdate VARCHAR(8) NOT NULL
            ,stockcode varchar(6) NOT NULL
            ,stockclose FLOAT NOT NULL
            ,stockopen FLOAT NOT NULL 
            ,stockhigh FLOAT NOT NULL
            ,stocklow FLOAT NOT NULL
            ,stockvolume FLOAT NOT NULL
            ,stockpricevolume FLOAT NOT NULL
        )
        """
        cursor.execute(create_table_query)
    except : 
        pass 
    
    for stockcode in merge_list : 
        sql = f"""
        INSERT INTO all_stock
        SELECT DISTINCT
               stockdate, '{stockcode}' as stockcode, stockclose, stockopen, stockhigh, stocklow, stockvolume, stockpricevolume
        FROM `{stockcode}`
        """
        
        cursor.execute(sql)
        conn.commit()