import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("hello-world")
        .master("local")
        .getOrCreate()
)
sc = spark.sparkContext
logFile = sys.argv[1]
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: {}, lines with b: {}".format(numAs, numBs))
spark.stop()
