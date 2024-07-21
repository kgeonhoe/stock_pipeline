
# %%
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, to_date, date_format
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.window import Window
import cf 
import pymysql
import os_setting
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType

# Spark 세션 초기화
import os 
# 타임아웃 설정
os.environ["PYDEVD_WARN_EVALUATION_TIMEOUT"] = "10.0"
os.environ["PYDEVD_UNBLOCK_THREADS_TIMEOUT"] = "10.0"
os.environ["PYDEVD_THREAD_DUMP_ON_WARN_EVALUATION_TIMEOUT"] = "True"
os.environ["PYDEVD_INTERRUPT_THREAD_TIMEOUT"] = "10.0"

# jdbc_jar_path = '/root/project/de-2024/resources/mariadb-java-client-2.7.4.jar'
file_path = os_setting.airflow_file_path_setting()
jdbc_jar_path = file_path + '/resources/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar'
print(jdbc_jar_path)
spark = (SparkSession.builder
    .master("local")
    .appName("Pandas_to_Spark")
    .config("spark.driver.extraClassPath", jdbc_jar_path)
    .config("spark.jars", jdbc_jar_path)
    .config("spark.executor.memory", "4g")  
    .config("spark.driver.memory", "4g")    
    .config("spark.sql.shuffle.partitions", "2") 
    .config("spark.default.parallelism", "2") 
    .getOrCreate())
#%%
    ## 아래 주석처리하여 spark submit 에있는 파일을 그대로 씀. 
    
    # 아래는 python 에서 실행할때
    # .config("spark.driver.memory", "4g") \
    # .config("spark.executor.memory", "4g") \
    # .config("spark.sql.shuffle.partitions", "50") \
        
def create_eavg_schema(w):
    return StructType([
        StructField("stockdate", DateType()),
        StructField("stockcode", StringType()),
        StructField("stockclose", LongType()),
        StructField(f"EAVG_Close_{w}", DoubleType())
    ])

def create_eavg_vol_schema(w):
    return StructType([
        StructField("stockdate", DateType()),
        StructField("stockcode", StringType()),
        StructField("stockvolume", LongType()),
        StructField(f"EAVG_Volume_{w}", DoubleType())
    ])

def eavg_pandas_udf(w):
    @pandas_udf(create_eavg_schema(w), PandasUDFType.GROUPED_MAP)
    def _eavg_pandas_udf(pdf):
        pdf = pdf.sort_values(by='stockdate')
        pdf[f'EAVG_Close_{w}'] = pdf['stockclose'].ewm(span=w).mean()
        return pdf[['stockdate', 'stockcode', 'stockclose', f'EAVG_Close_{w}']]
    return _eavg_pandas_udf

def eavg_vol_pandas_udf(w):
    @pandas_udf(create_eavg_vol_schema(w), PandasUDFType.GROUPED_MAP)
    def _eavg_vol_pandas_udf(pdf):
        pdf = pdf.sort_values(by='stockdate')
        pdf[f'EAVG_Volume_{w}'] = pdf['stockvolume'].ewm(span=w).mean()
        return pdf[['stockdate', 'stockcode', 'stockvolume', f'EAVG_Volume_{w}']]
    return _eavg_vol_pandas_udf

@pandas_udf("double", PandasUDFType.SCALAR)
def disparty_udf(a, b):
    return a / b * 100

### NOTE 
if __name__ == "__main__":
    
    database = 'airflow_daily_craw'
    jdbc_url = f"jdbc:mysql://{cf.host}:{cf.port}/{database}"

    jdbcdf =(spark.read 
            .format("jdbc")
            .option("url" , jdbc_url)
            .option('user' , cf.username)
            .option('password',cf.password)
            .option("fetchsize",1000)
            .option('dbtable','all_stock').load())
            # .option('partitionColumn', "stockcode")\
            # .option("lowerBound", "1") \
            # .option("upperBound", "1000000") \
            # .option('numPartitions',5)\
    
    jdbcdf = jdbcdf.withColumn("stockdate",to_date(col("stockdate"),"yyyyMMdd"))
    jdbcdf = jdbcdf.withColumn("stockclose", col("stockclose").cast(LongType()))
    jdbcdf = jdbcdf.withColumn("stockvolume", col("stockvolume").cast(LongType()))

    from pyspark.sql.functions import hash
    partitioned_df = jdbcdf.repartition(20,hash(col('stockcode')))
    
    
    tbl_name = 'processed_all_stock'
    import pymysql 
    conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
    cursor = conn.cursor()
    try : 
                # with self.cursor as cursor : 
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{tbl_name}` (
                stockdate VARCHAR(8) NOT NULL
                ,stockcode varchar(6) NOT NULL
                ,EAVG_Close_5 FLOAT 
                ,EAVG_Volume_5 FLOAT 
                ,Disparty_5 FLOAT 
                ,EAVG_Close_20 FLOAT 
                ,EAVG_Volume_20 FLOAT 
                ,Disparty_20 FLOAT 
                ,EAVG_Close_60 FLOAT 
                ,EAVG_Volume_60 FLOAT 
                ,Disparty_60 FLOAT 
                ,EAVG_Close_112 FLOAT 
                ,EAVG_Volume_112 FLOAT 
                ,Disparty_112 FLOAT 
                ,EAVG_Close_224 FLOAT 
                ,EAVG_Volume_224 FLOAT 
                ,Disparty_224 FLOAT 
                ,EAVG_Close_448 FLOAT 
                ,EAVG_Volume_448 FLOAT 
                ,Disparty_448 FLOAT 
                )
                """
                cursor.execute(create_table_query)
                conn.commit()
    except : 
        pass 
    
    
#%%
# Pandas UDF 사용

    # 여러 개의 w 값을 사용하여 컬럼 추가
    ws = [5, 20, 60, 112, 224, 448]

    for w in ws:
        result_df_close = partitioned_df.groupBy("stockcode").apply(eavg_pandas_udf(w))
        result_df_vol = partitioned_df.groupBy("stockcode").apply(eavg_vol_pandas_udf(w))
        
        # 결과를 원래 DataFrame과 조인
        partitioned_df = partitioned_df.join(result_df_close, on=["stockdate", "stockcode", "stockclose"], how="left")
        partitioned_df = partitioned_df.join(result_df_vol, on=["stockdate", "stockcode", "stockvolume"], how="left")
        partitioned_df = partitioned_df.withColumn(f"Disparty_{w}", disparty_udf(col("stockclose"), col(f"EAVG_Close_{w}")))

    # 날짜를 내림차순으로 정렬
    partitioned_df = partitioned_df.orderBy(col("stockdate").desc())
    # %%


    existing_df = spark.read \
            .format("jdbc")\
            .option("url" , jdbc_url)\
            .option('user' , cf.username)\
            .option('password',cf.password)\
            .option("fetchsize",1000)\
            .option('dbtable','processed_all_stock').load()

    existing_df = existing_df.withColumn("stockdate",to_date(col("stockdate"),"yyyyMMdd"))

    ## left _anti 조인을 활용하여 기존 테이블에 없는 새로운 데이터만 필터링 
    if existing_df.count() != 0 : 
        new_data_df = partitioned_df.join(existing_df, on=["stockdate", "stockcode"], how="left_anti")
    else : 
        new_data_df = partitioned_df
        
    new_data_df=new_data_df.select(
            'stockdate'
            ,'stockcode'
            ,'EAVG_Close_5'
            ,'EAVG_Volume_5'
            ,'Disparty_5'
            ,'EAVG_Close_20'
            ,'EAVG_Volume_20'
            ,'Disparty_20'
            ,'EAVG_Close_60'
            ,'EAVG_Volume_60'
            ,'Disparty_60'
            ,'EAVG_Close_112'
            ,'EAVG_Volume_112'
            ,'Disparty_112'
            ,'EAVG_Close_224'
            ,'EAVG_Volume_224'
            ,'Disparty_224'
            ,'EAVG_Close_448'
            ,'EAVG_Volume_448'
            ,'Disparty_448' 
        )
        
    new_data_df = new_data_df.withColumn("stockdate", date_format(col("stockdate"), "yyyyMMdd").cast(StringType()))
    new_data_df = new_data_df.withColumn("stockcode", col("stockcode").cast(StringType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_5", col("EAVG_Close_5").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_5", col("EAVG_Volume_5").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_5", col("Disparty_5").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_20", col("EAVG_Close_20").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_20", col("EAVG_Volume_20").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_20", col("Disparty_20").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_60", col("EAVG_Close_60").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_60", col("EAVG_Volume_60").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_60", col("Disparty_60").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_112", col("EAVG_Close_112").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_112", col("EAVG_Volume_112").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_112", col("Disparty_112").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_224", col("EAVG_Close_224").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_224", col("EAVG_Volume_224").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_224", col("Disparty_224").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Close_448", col("EAVG_Close_448").cast(FloatType()))
    new_data_df = new_data_df.withColumn("EAVG_Volume_448", col("EAVG_Volume_448").cast(FloatType()))
    new_data_df = new_data_df.withColumn("Disparty_448", col("Disparty_448").cast(FloatType()))

    new_data_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "processed_all_stock") \
        .option("user", cf.username) \
        .option("password", cf.password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    
    spark.stop()
    