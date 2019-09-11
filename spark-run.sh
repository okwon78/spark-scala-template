#!/bin/bash

if [ -z "$1" ]
then
    EMR_BUCKET_NAME=ap-rnd-recsys-apmall-spark-etl-stage
else
    EMR_BUCKET_NAME=$1
fi

echo EMR_BUCKET_NAME: $EMR_BUCKET_NAME

if [ -z "$2" ]
then
    RDB_ENDPOINT=jdbc:mysql://ap-rnd-recsys-apmall-service-stage.cluster-cgg3z66wlxpk.ap-northeast-2.rds.amazonaws.com
else
    RDB_ENDPOINT=$2
fi

echo RDB_ENDPOINT: $RDB_ENDPOINT

if [ -z "$3" ]
then
    RDB_DATABASE=apmall_service
else
    RDB_DATABASE=$3
fi

echo RDB_DATABASE: $RDB_DATABASE

if [ -z "$4" ]
then
    RDB_TABLE=popular
else
    RDB_TABLE=$4
fi

echo RDB_TABLE: $RDB_TABLE

if [ -z "$5" ]
then
    RDB_USER=amore
else
    RDB_USER=$5
fi

echo RDB_USER: $RDB_USER

if [ -z "$6" ]
then
    RDB_PWD=Amore12345!
else
    RDB_PWD=$6
fi

echo RDB_PWD: $RDB_PWD

if [ -z "$7" ]
then
    SOURCE_PATH=s3://ap-pipe-bdp-cdp-log/db_weblog/database/tb_recommend_raw/
else
    SOURCE_PATH=$7
fi

echo SOURCE_PATH: $SOURCE_BUCKET

if [ -z "$8" ]
then
    DAYS_AGO=20
else
    DAYS_AGO=$8
fi

echo DAYS_AGO: $DAYS_AGO

JAR_FILENAME=spark-scala-template-assembly-0.1.jar

aws s3 cp "s3://${EMR_BUCKET_NAME}/spark_jar/${JAR_FILENAME}" $PWD

spark-submit --master yarn \
             --deploy-mode cluster \
             --class SparkApp \
             $PWD/${JAR_FILENAME} \
             --rdb_endpoint ${RDB_ENDPOINT} \
             --rdb_database ${RDB_DATABASE} \
             --rdb_table ${RDB_TABLE} \
             --rdb_user ${RDB_USER} \
             --rdb_pwd ${RDB_PWD} \
             --days_ago ${DAYS_AGO} \
             --source_bucket ${SOURCE_PATH}
