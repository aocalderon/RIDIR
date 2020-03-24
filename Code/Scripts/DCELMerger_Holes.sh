#!/bin/bash

PARTITIONS=16
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=local[4]

A=/user/acald013/Datasets/Demo/H1.tsv
B=/user/acald013/Datasets/Demo/H2.tsv

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master $MASTER \
    --class DCELMerger $CLASS_JAR \
    --input1 $A --offset1 1 \
    --input2 $B --offset2 1 \
    --nlevels 2 --maxentries 1 --fraction 1 --custom \
    --local --save --debug
