#%%
from stock_filter import StockFilter
from collector import * 
import cf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
import pyarrow as pa 
from pathlib import Path
import argparse
import os_setting
import os 
import datetime 
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pymysql 
from dateutil.relativedelta import relativedelta
if __name__ == "__main__":
    file_path = os_setting.airflow_file_path_setting()
    collector = Collector(   appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)
    
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
    cursor = conn.cursor()
   
   
    parser = argparse.ArgumentParser() 
    args = parser.parse_args()
    
    args.spark = None
    args.conn = conn 
    args.cursor = cursor
    args.file_path = file_path
    filter = StockFilter(args)
    # table_list = filter.get_all_stock_mysql('airflow_daily_craw')
    stored_table_list = pd.read_sql('select distinct stockcode from all_stock' ,conn)
    all_stocks = filter.all_stock_list()
    dropcodetuple = tuple(set(stored_table_list.stockcode.to_list()) -set(all_stocks.stockCode.to_list())) 
    cursor.execute('delete from all_stock where stockcode in {}'.format(dropcodetuple))
    tenYrAgo = (datetime.datetime.now() - relativedelta(years=10)).strftime('%Y%m%d')
    cursor.execute("delete from all_stock where stockdate < '{}'".format(tenYrAgo))
    
    
    ####
    # for tbl in table_list : 
    #     filter.drop_table_mysql(tbl)
    ####
    
    
    