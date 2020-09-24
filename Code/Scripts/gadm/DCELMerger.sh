#!/bin/bash

PARTITIONS=32
EXECUTORS=12
CORES=9
DMEMORY=30g
EMEMORY=30g
DEBUG=""
SAVE=""
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
CLASS_NAME="DCELMerger"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=yarn
PARALLELISM=$((CORES * EXECUTORS * 10))

INPUT1=Datasets/Phili/phili_2000_2272.wkt
OFFSET1=2
INPUT2=Datasets/Phili/phili_2010_2272.wkt
OFFSET2=2
OUTPUT_PATH="/tmp"

while getopts "p:e:c:o:dl" OPTION; do
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
	MASTER="local[*]"
	;;
    o)
	OUTPUT_PATH=$OPTARG
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
     --conf spark.default.parallelism=${PARALLELISM} \
     --conf spark.driver.maxResultSize=2g \
     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}spark-measure_2.11-0.16.jar \
     --master $MASTER --deploy-mode client \
     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
     --class $CLASS_NAME $CLASS_JAR \
     --input1 $INPUT1 --offset1 $OFFSET1 --input2 $INPUT2 --offset2 $OFFSET2 --output $OUTPUT_PATH \
     --partitions $PARTITIONS $DEBUG 
