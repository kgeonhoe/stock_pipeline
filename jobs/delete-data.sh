#!/bin/bash

DATA_DIR=/opt/bitnami/spark/data

TARGET_DATE=$@
# TARGET_DATE=`date -v-1d "+%Y-%m-%d"`
echo ${TARGET_DATE}

TARGET_FILE=${TARGET_DATE}-$i.json.gz;
TARGET_URL=https://data.gharchive.org/${TARGET_FILE};
rm -f ${DATA_DIR}/${TARGET_FILE};
