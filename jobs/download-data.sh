#!/bin/bash

DATA_DIR=/opt/bitnami/spark/data

TARGET_DATE=$@
# TARGET_DATE=`date -v-1d "+%Y-%m-%d"`
echo ${TARGET_DATE}

# 데이터 디렉토리의 소유권 및 권한 설정
if [ ! -d "$DATA_DIR" ]; then
  mkdir -p $DATA_DIR
fi

# 현재 사용자가 해당 디렉토리에 쓸 수 있는지 확인하고, 필요한 경우 소유권 및 권한 변경
sudo chown -R $(whoami) $DATA_DIR
chmod -R 775 $DATA_DIR


for i in $(seq 1 23); 
do 
TARGET_FILE=${TARGET_DATE}-$i.json.gz;
TARGET_URL=https://data.gharchive.org/${TARGET_FILE};
wget ${TARGET_URL} -P ${DATA_DIR} --backups=0;
gunzip -f ${DATA_DIR}/${TARGET_DATE}-$i.json.gz;
rm -f ${DATA_DIR}/${TARGET_FILE};
done
