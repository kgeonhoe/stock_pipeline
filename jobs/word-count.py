from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("word-count")
        .master("local")
        .getOrCreate()
)
sc = spark.sparkContext

text = "Hello Spark Hello Python Hello Docker Hello World"
words = sc.parallelize(text.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()
