#!/bin/bash

PARTITIONS=1024
EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
DEBUG=""
SAVE=""
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=yarn

INPUT1=/user/acald013/Datasets/CA/cali2000_polygons6414.tsv
OFFSET1=2
INPUT2=/user/acald013/Datasets/CA/cali2010_polygons6414.tsv
OFFSET2=2
#INPUT1=$HOME/Datasets/WKT/PhiliA.wkt
#OFFSET1=0
#INPUT2=$HOME/Datasets/WKT/PhiliB.wkt
#OFFSET2=0

while getopts "p:e:c:d:l" OPTION; do
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
    --conf spark.default.parallelism=${PARTITIONS} \
    --conf spark.locality.wait=1s \
    --conf spark.locality.wait.node=0s \
    --conf spark.locality.wait.rack=0s \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class DCELMerger $CLASS_JAR \
    --input1 $INPUT1 --offset1 $OFFSET1 --input2 $INPUT2 --offset2 $OFFSET2 \
    --partitions $PARTITIONS $DEBUG 

