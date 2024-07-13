#!/bin/bash

# Initialize the Airflow database
airflow db init

# Migrate the Airflow database
airflow db upgrade

# Create an admin user
airflow users create --username airflow --firstname airflow --lastname airflow --role Admin --email airflow@gmail.com --password airflow

# Add Airflow connection
airflow connections add --conn_id=spark-conn --conn_type=spark --conn_extra='{ \"host\":\"spark://spark-master:7077\", \"deploy_mode\": \"client\", \"spark_binary\": \"spark-submit\" }'

# Execute the original command
exec "$@"
