#!/bin/bash

PARTITIONS=1080
EXECUTORS=12
CORES=9
DMEMORY=30g
EMEMORY=30g
DEBUG=""
SAVE=""
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
CLASS_NAME="FaceTester"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=local[*]
PARALLELISM=$((CORES * EXECUTORS * 10))

INPUT1=/user/acald013/gadm/level1
OFFSET1=0
INPUT2=/user/acald013/gadm/level2
OFFSET2=0
#INPUT1=$HOME/Datasets/WKT/PhiliA.wkt
#OFFSET1=0
#INPUT2=$HOME/Datasets/WKT/PhiliB.wkt
#OFFSET2=0

while getopts "p:e:c:dl" OPTION; do
    case $OPTION in
    p)
        PARTITIONS=$OPTARG
        ;;
    e)
	EXECUTORS=$OPTARG
	;;
    c)
	CORES=$OPTARG
	;;
    l)
	MASTER="local[${CORES}]"
	;;
    d)
	DEBUG="--debug"
	;;
    *)
        echo "Incorrect options provided"
        exit 1
        ;;
    esac
done

spark-submit \
     --files $LOG_FILE \
     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}geospark-viz_2.3-1.2.0.jar \
     --master $MASTER --deploy-mode client \
     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
     --class $CLASS_NAME $CLASS_JAR 

