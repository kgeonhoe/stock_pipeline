# import pandas as pd
# from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, DateType, IntegerType
import pyspark.sql.functions as F
# import pymysql

### 이동편균선 계산. 
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

@pandas_udf("double", PandasUDFType.SCALAR)
def disparty_udf(a, b):
    return a / b * 100