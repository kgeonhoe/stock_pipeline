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
from elasticsearch import Elasticsearch

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
    
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
    cursor = conn.cursor()
   
    try : 
        all_stocks = pd.read_json(file_path + '/data/'+ 'all_stocks.json')
        if not all_stocks.etldate[0] == datetime.datetime.now().strftime('%Y%m%d') : 
            all_stocks = collector.kis_get_all_stock()
            all_stocks['etldate'] = datetime.datetime.now().strftime('%Y%m%d') 
            all_stocks['etlcheck'] = None
            all_stocks.to_json(file_path + '/data/' + 'all_stocks.json')
    except : 
        all_stocks = collector.kis_get_all_stock()
        all_stocks['etldate'] = datetime.datetime.now().strftime('%Y%m%d') 
        all_stocks['etlcheck'] = None
        all_stocks.to_json(file_path + '/data/' + 'all_stocks.json')
        pass 
    # 엘라스틱 서치에 크롤링 할 테이블 넣기 
    
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
    es = Elasticsearch("http://localhost:9200")
    args.es = es
    args.file_path = file_path
    all_stocks_df = all_stocks[(all_stocks.etlcheck.isnull()) & (all_stocks.priceChange == '00')]
    all_stock_changed_df = all_stocks[(all_stocks.etlcheck.isnull()) & (all_stocks.priceChange != '00')]
    
    filter = StockFilter(args)
    # time.sleep(60)
    filter.idx_generate('crawl_tbl')
    for idx, df in enumerate([all_stocks_df, all_stock_changed_df]) : 
        for _, stock_data in df.iterrows(): 
            if idx == 1 : 
                filter.drop_es(stock_data.stockCode)
            if not filter.check_lastdate(stock_data.stockCode) == None : 
                datefrom = filter.check_lastdate(stock_data.stockCode)
                datefrom = (datetime.datetime.strptime(datefrom,'%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                
            else : 
                datefrom = '20000101'
                
            date_iter = filter.date_range(stock_data.stockCode, datefrom, datetime.datetime.now().strftime('%Y%m%d'))
            for stockCode, datefrom, dateto in date_iter: 
                filter.crawl(stockCode,datefrom, dateto)
            all_stocks.loc[all_stocks['stockCode'] == stock_data.stockCode, 'etlcheck'] =datetime.datetime.now()
            # filter.crawl(stockCode,datefrom, dateto)
    
    # for _, stock_data in 
    
    
    