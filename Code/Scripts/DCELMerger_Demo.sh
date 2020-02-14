#!/bin/bash

PARTITIONS=4
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=local[4]

A=/user/acald013/Datasets/Demo/B2.wkt
B=/user/acald013/Datasets/Demo/B3.wkt

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master $MASTER \
    --class DCELMerger $CLASS_JAR \
    --input1 $A --offset1 0 \
    --input2 $B --offset2 0 \
    --nlevels 2 --maxentries 1 --fraction 1 --custom \
    --local --save --debug
