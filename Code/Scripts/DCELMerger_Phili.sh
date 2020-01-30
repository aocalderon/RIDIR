#!/bin/bash

PARTITIONS=128
EXECUTORS=10
CORES=4
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=yarn

PHILI1=/user/acald013/Datasets/Phili/phili_2000_2272.wkt
PHILI2=/user/acald013/Datasets/Phili/phili_2010_2272.wkt

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master $MASTER \
    --deploy-mode client \
    --num-executors $EXECUTORS \
    --executor-cores $CORES \
    --driver-memory 16g \
    --executor-memory 12g \
    --class DCELMerger $CLASS_JAR \
    --input1 $PHILI1 --offset1 2 \
    --input2 $PHILI2 --offset2 2 \
    --nlevels 8 --maxentries 50 --fraction 0.25 \
    --partitions $PARTITIONS \
    --save --debug
