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
CLASS_NAME="DCELTester"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=yarn
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

#    --conf spark.scheduler.mode=FAIR \
#    --conf spark.locality.wait=1s \
#    --conf spark.locality.wait.node=0s \
#    --conf spark.locality.wait.rack=0s \
spark-submit \
     --files $LOG_FILE \
     --conf spark.default.parallelism=${PARALLELISM} \
     --conf spark.driver.maxResultSize=4g \
     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}spark-measure_2.11-0.16.jar \
     --master $MASTER --deploy-mode client \
     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
     --class $CLASS_NAME $CLASS_JAR \
     --input1 $INPUT1 --offset1 $OFFSET1 --input2 $INPUT2 --offset2 $OFFSET2 \
     --partitions $PARTITIONS $DEBUG 

