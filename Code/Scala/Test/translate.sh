#!/bin/bash

EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/Scala/Test/target/scala-2.11/tester_2.11-0.1.jar
CLASS_NAME="Translate"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

THE_MASTER="yarn"
THE_INPUT="hdfs:///user/acald013/WKT/provinces"
THE_OUTPUT="hdfs:///user/acald013/WKT/provinces2"
THE_PARTITIONS=1080
THE_SOURCE="EPSG:3857"
THE_TARGET="EPSG:3857"

while getopts "i:o:p:s:t:l" OPTION; do
    case $OPTION in
    i)
        THE_INPUT=$OPTARG
        ;;
    o)
        THE_OUTPUT=$OPTARG
        ;;
    p)
        THE_PARTITIONS=$OPTARG
        ;;
    s)
        THE_SOURCE=$OPTARG
        ;;
    t)
        THE_TARGET=$OPTARG
        ;;
    l)
	THE_MASTER="local[*]"
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
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}geospark-viz_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}proj4j-1.1.1.jar \
    --master $THE_MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS_NAME $CLASS_JAR --input $THE_INPUT --output $THE_OUTPUT --partitions $THE_PARTITIONS --source $THE_SOURCE --target $THE_TARGET

