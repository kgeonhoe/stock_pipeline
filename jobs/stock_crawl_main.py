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
import logging
import os_setting

                    
#%%
if __name__ == "__main__":
    
    ## NOTE data 파일경로 잡아주는 목적 
    file_path = os_setting.airflow_file_path_setting()
    collector = Collector(   appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
    cursor = conn.cursor()
   
    ## NOTE SPARK 객체 생성 
    ## 여기서는 스파크 객체를 사용할 일이 없음. 
    
    # spark =  (SparkSession
    #          .builder
    #          .master("local")
    #          .appName("es-test")
    #          .config("spark.driver.extraClassPath", file_path + "/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    #          .config("spark.jars", file_path + "/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
    #          .getOrCreate())
    spark = None 
    
    parser = argparse.ArgumentParser() 
    args = parser.parse_args()
    args.spark = spark
    args.conn = conn 
    args.cursor = cursor
    args.file_path = file_path
    
    
    filter = StockFilter(args)
    all_stocks = filter.all_stock_list()
    
    ## 액면분할 안된 애들 
    all_stocks_df = all_stocks[((all_stocks.etlcheck== '')|(all_stocks.etlcheck.isnull())) & (all_stocks.priceChange == '00')]
    ## 액면 분할 된애들 
    all_stock_changed_df = all_stocks[((all_stocks.etlcheck== '')|(all_stocks.etlcheck.isnull())) & (all_stocks.priceChange != '00')]
    insert_query = """INSERT IGNORE INTO all_stock_list (
                 stockCode
                ,stockName
                ,priceChange
                ,listeddate
                ,etldate
                ,etlcheck
            )
            VALUES (%s,%s,%s,%s,%s,%s)"""
            
    last_sql = """
    select stockcode, max(stockdate) as stockdate from all_stock 
    group by stockcode
    """
    df_lastdate_mysql = filter.last_date_df()
    now = datetime.datetime.now()
    time.sleep(90)
    for idx, df in enumerate([all_stocks_df, all_stock_changed_df]) : 
        print('전체 크롤링 숫자 : ',len(df))
        for _, stock_data in df.iterrows(): 
            ## 액면분할이 되었을 경우
            if idx == 1 : 
                filter.delete_data_mysql(stock_data.stockCode, '0', now.strftime('%Y%m%d'))        
            try : 
                iterdatefrom = df_lastdate_mysql.loc[df_lastdate_mysql['stockcode'] == stock_data.stockCode, 'stockdate'].values[0]
            except : 
                if datetime.datetime.strptime(stock_data.listeddate,'%Y%m%d') > now - relativedelta(years=10) : 
                    iterdatefrom = stock_data.listeddate 
                else : 
                    iterdatefrom = (now - relativedelta(years=10)).strftime('%Y%m%d')
                    
            date_iter = filter.date_range(stock_data.stockCode, iterdatefrom, datetime.datetime.now().strftime('%Y%m%d'))
            
            ## 여기서 
            for collect_idx, data in enumerate(date_iter): 
                stockCode, datefrom, dateto = data[0], data[1], data[2]
                crawled_data = filter.crawl_mysql(stockCode,datefrom, dateto)
                ## 마지막 
                try : 
                    if dateto == df_lastdate_mysql.loc[df_lastdate_mysql['stockcode'] == stockCode, 'stockdate'].values[0] : 
                        print(stockCode, '이미 크롤링되어 수집할 데이터 없습니다.')
                        continue
                except : 
                    # logging.error("An error Occured : %s", e)
                    pass 
                ### 액면분할 항목 아닌 종목은 다시 체크 
                if collect_idx == 0 : 
                    ##  crawled_data[0][-1] : stockdate
                    ##  crawled_data[0][-1] : stockclose
                    try : 
                        if (df_lastdate_mysql.loc[df_lastdate_mysql['stockcode'] == stockCode, 'stockdate'] == crawled_data[0][-1]).values[0] & (df_lastdate_mysql.loc[df_lastdate_mysql['stockcode'] == stockCode, 'stockclose'] != crawled_data[1][-1]).values[0] : 
                            changedtempdf = pd.DataFrame(dict(zip(['stockCode', 'stockName', 'priceChange', 'lastdate', 'etldate', 'etlcheck'], [stockCode, None, '01', None, datetime.datetime.now().strftime('%Y%m%d'), None])), index = range(len([stockCode])))
                            all_stock_changed_df = pd.concat([all_stock_changed_df, changedtempdf]).reset_index(drop=True)
                            filter.delete_data_mysql(stock_data.stockCode, '0', now.strftime('%Y%m%d'))
                            continue
                            
                        ### 
                        else : 
                            filter.insert_sql(stockCode, crawled_data, 'all_stock' )
                    
                    except : 
                        pass 
                
            all_stocks.loc[all_stocks['stockCode'] == stock_data.stockCode, 'etlcheck'] = now.strftime('%Y%m%d')
            print('증권번호', stock_data.stockCode, _,'번째 크롤링 완료' ,  '총수량 :', len(df))
        ## all-stocks 바꿔주기 
            cursor.execute(f"update all_stock_list set etlcheck = {now.strftime('%Y%m%d')} where stockCode = {stock_data.stockCode}")
            conn.commit()
        # for row in all_stocks.itertuples(index=False, name=None):
            # cursor.cursor.execute(insert_query, row)
        
        print()

    conn.close()
            
    
    