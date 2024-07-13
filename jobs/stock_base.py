from abc import ABC, abstractmethod
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark.sql.functions as F
from collector import * 



class BaseFilter(ABC): 
    def __init__(self, args) : 
        self.args = args 
        self.spark = args.spark 
         
    def filter(self, df) : 
        pass 
