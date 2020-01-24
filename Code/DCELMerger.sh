#!/bin/bash

PARTITIONS=$1
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=yarn
CORES=4
EXECUTORS=12

PHILI1=/user/acald013/Datasets/Phili/phili_2000_2272.wkt
PHILI2=/user/acald013/Datasets/Phili/phili_2010_2272.wkt
CA1=/user/acald013/Datasets/CA/cali2000_polygons6414.tsv
CA2=/user/acald013/Datasets/CA/cali2010_polygons6414.tsv

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
    --input1 $CA1 --offset1 2 \
    --input2 $CA2 --offset2 2 \
    --nlevels 10 --maxentries 100 --fraction 0.25 --partitions $PARTITIONS \
    --save --debug
