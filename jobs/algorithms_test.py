
# %%
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql.window import Window
import cf 
import pymysql
# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("Pandas_to_Spark") \
    .getOrCreate()

# MySQL에서 데이터 불러오기
conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
pdf1 = pd.read_sql("SELECT * FROM `005930`", conn)
pdf1['code'] = '005930'
pdf2 = pd.read_sql("SELECT * FROM `000020`", conn)
pdf2['code'] = '000020'
# 데이터 타입 변환
pdf1['stockdate'] = pd.to_datetime(pdf1['stockdate'])
pdf2['stockdate'] = pd.to_datetime(pdf2['stockdate'])

# Pandas DataFrame을 Spark DataFrame으로 변환
df1 = spark.createDataFrame(pdf1)
df2 = spark.createDataFrame(pdf2)

# 두 개의 Spark DataFrame 합치기
df = df1.union(df2)

# Pandas UDF 사용
schema_eavg = StructType([
    StructField("stockdate", DateType()),
    StructField("code", StringType()),
    StructField("stockclose", IntegerType()),
    StructField("EAVG_Close", DoubleType())
])

schema_eavg_vol = StructType([
    StructField("stockdate", DateType()),
    StructField("code", StringType()),
    StructField("stockvolume", IntegerType()),
    StructField("EAVG_Volume", DoubleType())
])

@pandas_udf(schema_eavg, PandasUDFType.GROUPED_MAP)
def eavg_pandas_udf(pdf):
    pdf = pdf.sort_values(by='stockdate')
    pdf['EAVG_Close'] = pdf['stockclose'].ewm(span=20).mean()
    return pdf[['stockdate', 'code', 'stockclose', 'EAVG_Close']]

@pandas_udf(schema_eavg_vol, PandasUDFType.GROUPED_MAP)
def eavg_vol_pandas_udf(pdf):
    pdf = pdf.sort_values(by='stockdate')
    pdf['EAVG_Volume'] = pdf['stockvolume'].ewm(span=20).mean()
    return pdf[['stockdate', 'code', 'stockvolume', 'EAVG_Volume']]

# 'code'를 기준으로 그룹화하고, Pandas UDF를 적용
result_df_close = df.groupBy("code").apply(eavg_pandas_udf)
result_df_vol = df.groupBy("code").apply(eavg_vol_pandas_udf)

# 결과를 원래 DataFrame과 조인
df = df.join(result_df_close, on=["stockdate", "code", "stockclose"], how="left")
df = df.join(result_df_vol, on=["stockdate", "code", "stockvolume"], how="left")

# Disparty 계산
@pandas_udf("double", PandasUDFType.SCALAR)
def disparty_udf(a, b):
    return a / b * 100

df = df.withColumn("Disparty", disparty_udf(col("stockclose"), col("EAVG_Close")))
df = df.orderBy(col("stockdate").desc())
df.show()


# %%
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, IntegerType
import pyspark.sql.functions as F
import pymysql


# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("Pandas_to_Spark") \
    .getOrCreate()

# MySQL에서 데이터 불러오기
conn = pymysql.connect(host=cf.host, user=cf.username, passwd=cf.password, port=cf.port,database='airflow_daily_craw', use_unicode=True, charset='utf8')
pdf1 = pd.read_sql("SELECT * FROM `005930`", conn)
pdf1['code'] = '005930'
pdf2 = pd.read_sql("SELECT * FROM `000020`", conn)
pdf2['code'] = '000020'

# 데이터 타입 변환
pdf1['stockdate'] = pd.to_datetime(pdf1['stockdate'])
pdf2['stockdate'] = pd.to_datetime(pdf2['stockdate'])

# Pandas DataFrame을 Spark DataFrame으로 변환
df1 = spark.createDataFrame(pdf1)
df2 = spark.createDataFrame(pdf2)

# 두 개의 Spark DataFrame 합치기
df = df1.union(df2)
# Pandas UDF 사용
def create_eavg_schema(w):
    return StructType([
        StructField("stockdate", DateType()),
        StructField("code", StringType()),
        StructField("stockclose", IntegerType()),
        StructField(f"EAVG_Close_{w}", DoubleType())
    ])

def create_eavg_vol_schema(w):
    return StructType([
        StructField("stockdate", DateType()),
        StructField("code", StringType()),
        StructField("stockvolume", IntegerType()),
        StructField(f"EAVG_Volume_{w}", DoubleType())
    ])

def eavg_pandas_udf(w):
    @pandas_udf(create_eavg_schema(w), PandasUDFType.GROUPED_MAP)
    def _eavg_pandas_udf(pdf):
        pdf = pdf.sort_values(by='stockdate')
        pdf[f'EAVG_Close_{w}'] = pdf['stockclose'].ewm(span=w).mean()
        return pdf[['stockdate', 'code', 'stockclose', f'EAVG_Close_{w}']]
    return _eavg_pandas_udf

def eavg_vol_pandas_udf(w):
    @pandas_udf(create_eavg_vol_schema(w), PandasUDFType.GROUPED_MAP)
    def _eavg_vol_pandas_udf(pdf):
        pdf = pdf.sort_values(by='stockdate')
        pdf[f'EAVG_Volume_{w}'] = pdf['stockvolume'].ewm(span=w).mean()
        return pdf[['stockdate', 'code', 'stockvolume', f'EAVG_Volume_{w}']]
    return _eavg_vol_pandas_udf
# 여러 개의 w 값을 사용하여 컬럼 추가
ws = [5, 20, 60, 112, 224, 448]

for w in ws:
    result_df_close = df.groupBy("code").apply(eavg_pandas_udf(w))
    result_df_vol = df.groupBy("code").apply(eavg_vol_pandas_udf(w))
    
    # 결과를 원래 DataFrame과 조인
    df = df.join(result_df_close, on=["stockdate", "code", "stockclose"], how="left")
    df = df.join(result_df_vol, on=["stockdate", "code", "stockvolume"], how="left")

   
    @pandas_udf("double", PandasUDFType.SCALAR)
    def disparty_udf(a, b):
        return a / b * 100

    df = df.withColumn(f"Disparty_{w}", disparty_udf(col("stockclose"), col(f"EAVG_Close_{w}")))

# 날짜를 내림차순으로 정렬
df = df.orderBy(col("stockdate").desc())

df.show()

# %%
