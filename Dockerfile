FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && apt-get install -y vim
RUN apt-get install -y gcc python3-dev openjdk-11-jdk wget
RUN apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
# RUN chmod +x /jobs/spark-submit.sh

######################
# Copy the spark-submit.sh script
# COPY ./jobs/spark-submit.sh /opt/jobs/spark-submit.sh
# RUN chmod +x /opt/jobs/spark-submit.sh

# Copy the entrypoint script
# COPY entrypoint.sh /entrypoint.sh
# RUN dos2unix entrypoint.sh
# RUN chmod +x /entrypoint.sh
######################

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark elasticsearch 
RUN pip3 install pandas numpy python-kis pymysql


# Set the entrypoint to the custom script
# ENTRYPOINT ["/entrypoint.sh"]
