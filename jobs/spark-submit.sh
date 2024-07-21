#!/bin/bash

# JARS="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar"
# 아래가 mysql 연결 파일
JARS="/opt/bitnami/spark/resources/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar"

JOBNAME="RefinePipeline"
SCRIPT=$@
echo ${SCRIPT}

spark-submit \
  --name ${JOBNAME} \
  --master spark://spark-master:7077 \
  --jars ${JARS} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=2m \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=3 \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2G \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.executor.memory=4G \
  --conf spark.driver.memory=4G \
  --conf spark.sql.shuffle.partitions=50 \
  --conf spark.driver.maxResultSize=0 \
  --conf spark.driver.extraClassPath=${JARS} \
  --conf spark.jars=${JARS} \
  --num-executors 2 \
  --executor-cores 1 \
  ${SCRIPT}
