#!/bin/bash

EXECUTORS=12
CORES=9
DMEMORY=30g
EMEMORY=30g

SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
CLASS_NAME="DCELMerger2"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=local[8]

INPUT1="file:///${HOME}/RIDIR/Datasets/edgesDemo.wkt"
OFFSET1=0

spark-submit \
     --files $LOG_FILE \
     --conf spark.driver.maxResultSize=2g \
     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}spark-measure_2.11-0.16.jar \
     --master $MASTER --deploy-mode client \
     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
     --class $CLASS_NAME $CLASS_JAR \
     --input1 $INPUT1 --offset1 $OFFSET1 --local   

