from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from collector import * 
from stock_base import BaseFilter
import cf 
import datetime

stock_schema = StructType([  
                 StructField('stockdate', StringType(), True)
                ,StructField('stockclose', LongType(), True)
                ,StructField('stockopen', LongType(), True)
                ,StructField('stockhigh', LongType(), True)
                ,StructField('stocklow', LongType(), True)
                ,StructField('stockvolume', LongType(), True)
                ,StructField('stockpricevolume', LongType(), True)
                                ])


class StockFilter(BaseFilter): 
    def __init__(self, args) : 
        self.args = args 
        self.spark = args.spark 
        # self.es = args.es
        self.conn = args.conn 
        self.cursor = args.cursor 
        self.file_path = args.file_path
        self.collector =  Collector(appkey= cf.appkey
                            ,appsecret= cf.appsecret
                            ,virtual_accountYN = True)
            
        
    def date_range(self, code, datefrom, dateto) : 
        date_ranges = self.collector.generate_date_ranges(datefrom, dateto, 30)
        date_data = [(code, date_range[0], date_range[1]) for date_range in date_ranges]
        return date_data
        
        # db 에 저장되어있는 마지막 날짜를 가지고 오기 
    
    def check_lastdate_mysql(self,code) : 
        try :
            last_date_query = f"""
                SELECT MAX(stockdate) FROM `{code}`
            """
            df = pd.read_sql(last_date_query,self.conn)
            return df.iloc[0,0]
        except : 
            all_stocks = self.collector.kis_get_all_stock()
            all_stocks['etldate'] = datetime.datetime.now().strftime('%Y%m%d') 
            all_stocks['etlcheck'] = None
            all_stocks.to_json(self.file_path + '/data/' + 'all_stocks.json')
        
        return all_stocks
        
    def get_all_stock_mysql(self, schema_name) : 
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s"
        self.cursor.execute(query, (schema_name,))
        tables = self.cursor.fetchall()
        table_list = [table[0] for table in tables]
        return table_list



    def delete_data_mysql(self, code, date_from, date_to) : 
        """_summary_
        특정 기간 데이터를 지우는 함수 
        Args:
            code (_type_): _description_
            date_from (_type_): _description_
            date_to (_type_): _description_
        """        
        try : 
            query = f"""
            DELETE FROM `{code}` where stockdate bewteen {date_from} AND {date_to}
            """
            self.cursor.execute(query)
        except : 
            pass 
        
    def drop_table_mysql(self, code) : 
        try : 
            query = f""" 
            DROP TABLE IF EXISTS `{code}`
            """ 
            self.cursor.execute(query)
        except : 
            pass 
    
    def crawl_mysql(self,code,datefrom,dateto) :         
        # if not self.check_lastdate_mysql(code).iloc[0,0] == None : 
        #     datefrom = self.check_lastdate_mysql(code).iloc[0,0]
            
        # for code, datefrom ,dateto in self.date_range(code, datefrom, dateto) : 
        data = self.collector.kis_get_values(code, datefrom, dateto)
        return data 
    
    def insert_sql(self, stockcode, data, db_name) : 
        data = (i[:-1] for i in data)
        df = pd.DataFrame(dict(zip(['stockdate', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume'],data)))
        df['stockcode'] = stockcode 
        df = df[['stockdate','stockcode', 'stockclose', 'stockopen', 'stockhigh', 'stocklow', 'stockvolume', 'stockpricevolume']]
        df = df.sort_values('stockdate').reset_index(drop=True)
        try : 
            # with self.cursor as cursor : 
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{db_name}` (
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
            self.cursor.execute(create_table_query)
            ## INSERT IGNORE 의 경우 기본키나 고유키 제약조건을 사용하여 중복 감지
            
            
            insert_query = f"""
            INSERT IGNORE INTO `{db_name}` (
                stockdate
                ,stockcode
                ,stockclose
                ,stockopen
                ,stockhigh
                ,stocklow
                ,stockvolume
                ,stockpricevolume
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for row in df.itertuples(index=False, name=None):
                self.cursor.execute(insert_query, row)
          
            # 커밋 
            self.conn.commit()
                
        finally : 
            pass             
            # new_spark_df = self.spark.createDataFrame(df, stock_schema)
            # new_spark_df.to_sql
            # combined_df = df_origin.union(new_spark_df)
    
    