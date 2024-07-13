#%%
from filter import filter
from collector import * 
import cf
package_path = '/root/project/de-2024/data/stocks'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
import pyarrow as pa 
from pathlib import Path

# collector = Collector(   appkey= cf.appkey
#                         ,appsecret= cf.appsecret
#                         ,virtual_accountYN = True)

# all_stocks = collector.kis_get_all_stock()
#     # FIXME
# all_stocks = all_stocks[all_stocks['stockCode'] == '005930']

#     # Spark 세션 생성
# spark = SparkSession.builder \
#     .appName("ReadParquet") \
#     .getOrCreate()

# import os 
# import subprocess 

# def is_file_exists(file_path):
#     return Path(file_path).is_file()

if __name__ == "__main__":
    filter()
  
    # all_stocks = all_stocks[all_stocks['stockCode'] == '005930']

    # stockCode = all_stocks.stockCode
    # stockName = all_stocks.stockName
    # priceChange = all_stocks.priceChange
    # # package_path 에 파일이 있는지 확인 
    # import os 

    # file_path = package_path + '/'  + f'{stockCode.values[0]}.parquet'
    # if is_file_exists(file_path):
    #     print(f"'{file_path}'이(가) 존재합니다.")
    #     df_origin = spark.read.parquet(file_path)
    #     selected_columns = ['stockdate']
    #     data_selected = df_origin.select(*selected_columns)
    #     # 가장 큰 값 찾기
    #     max_value = data_selected.agg(max(col("stockdate")).alias("max_value")).collect()[0]["max_value"]
    #     date_ranges = collector.generate_date_ranges(max_value, datetime.now().strftime('%Y%m%d'), 30)
    #     date_data = [(stockCode.values[0], date_range[0], date_range[1]) for date_range in date_ranges]

    #     for code, datefrom, dateto in date_data : 
    #         data = collector.kis_get_values(code, datefrom,dateto)
    #         if data == None : 
    #             continue
    #         else :
    #             df = pd.DataFrame(dict(zip(['stockdate', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume'],data)))
    #             new_spark_df = spark.createDataFrame(df)
    #             new_spark_df.write.mode("append").parquet(file_path)
       
        
    # else : 
    #     print('디렉토리에 파일이없습니다. 처음부터 collect')
    #     date_ranges = collector.generate_date_ranges('19850101', datetime.now().strftime('%Y%m%d'), 30)
    #     date_data = [(stockCode.values[0], date_range[0], date_range[1]) for date_range in date_ranges]

    #     for code, datefrom, dateto in date_data : 
    #         data = collector.kis_get_values(code, datefrom,dateto)
    #         if data == None : 
    #             continue
    #         else :
    #             df = pd.DataFrame(dict(zip(['stockdate', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume'],data)))
    #             df.to_parquet(file_path)
       
    
# %%
