#!/bin/bash

SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
CORES=3
EXECUTORS=40

spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --master yarn \
    --deploy-mode client \
    --num-executors $EXECUTORS \
    --executor-cores $CORES \
    --driver-memory 16g \
    --executor-memory 12g \
    --class DCELMerger $CLASS_JAR \
    --input1 /user/acald013/Datasets/Phili/phili_2000_2272.wkt --offset1 2 \
    --input2 /user/acald013/Datasets/Phili/phili_2010_2272.wkt --offset2 2 \
    --nlevels 8 --maxentries 100 --fraction 0.1 \
    --save --debug
