#!/bin/bash

HDFS_PATH=$1
LOCAL_PATH=$2

EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/sdcel_2.11-0.1.jar
CLASS="edu.ucr.dblab.sdcel.SDCEL"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=yarn

spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}spark-measure_2.11-0.16.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS $JAR \
    --input1 $HDFS_PATH/edgesA \
    --input2 $HDFS_PATH/edgesB \
    --quadtree $LOCAL_PATH/quadtree.wkt \
    --boundary $LOCAL_PATH/boundary.wkt \
    --scale 10000 
