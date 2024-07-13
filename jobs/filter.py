from collector import * 
import cf
package_path = '/root/project/de-2024/data/stocks'
# package_path = '/opt/bitnami/spark/data/stocks'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
import pyarrow as pa 
from pathlib import Path
import logging 
logging.basicConfig(level=logging.INFO)


def is_file_exists(file_path):
    return Path(file_path).is_file()

def filter() : 
    collector = Collector(   appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)

    all_stocks = collector.kis_get_all_stock()
    # FIXME
    all_stocks = all_stocks[all_stocks['stockCode'] == '005930']

        # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("ReadParquet") \
        .getOrCreate()
  

    stockCode_list = all_stocks.stockCode
    stockName_list = all_stocks.stockName
    priceChange_list = all_stocks.priceChange
    # package_path 에 파일이 있는지 확인 
    import os 
    stockCode = stockCode_list.values[0]
    
    
    file_path = package_path + '/'  + f'{stockCode}.parquet'

    if is_file_exists(file_path):
        print(f"'{file_path}'이(가) 존재합니다.")
        df_origin = spark.read.parquet(file_path)
        selected_columns = ['stockdate']
        data_selected = df_origin.select(*selected_columns)
        # 가장 큰 값 찾기
        max_value = data_selected.agg(max(col("stockdate")).alias("max_value")).collect()[0]["max_value"]
        date_ranges = collector.generate_date_ranges((datetime.strptime(max_value,'%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'), datetime.now().strftime('%Y%m%d'), 30)
        date_data = [(stockCode, date_range[0], date_range[1]) for date_range in date_ranges]

        for code, datefrom, dateto in date_data : 
            data = collector.kis_get_values(code, datefrom,dateto)
            if data == None : 
                continue
            else :
                df = pd.DataFrame(dict(zip(['stockdate', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume'],data)))
                new_spark_df = spark.createDataFrame(df)
                combined_df = df_origin.union(new_spark_df)
            
                combined_df.write.parquet(package_path + '/modified_'  + f'{stockCode}.parquet')
                
                os.remove(file_path)
                os.rename(package_path + '/modified_'  + f'{stockCode}.parquet', file_path)
                os.remove(package_path + '/modified_'  + f'{stockCode}.parquet')
                # new_spark_df.write.mode("append").parquet(package_path + '/modified_'  + f'{stockCode}.parquet')
        
        
    else : 
        print(f"'{file_path}'이(가) 존재하지 않습니다.")
        print('디렉토리에 파일이없습니다. 처음부터 collect')
        date_ranges = collector.generate_date_ranges('19850101', datetime.now().strftime('%Y%m%d'), 30)
        date_data = [(stockCode, date_range[0], date_range[1]) for date_range in date_ranges]

        for code, datefrom, dateto in date_data : 
            data = collector.kis_get_values(code, datefrom,dateto)
            if data == None : 
                continue
            else :
                df = pd.DataFrame(dict(zip(['stockdate', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume'],data)))
                df.to_parquet(file_path)
        