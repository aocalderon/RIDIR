#!/bin/bash

MASTER=yarn
EXECUTORS=12
CORES=8
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/Scala/Test/target/scala-2.11/tester_2.11-0.1.jar
CLASS_NAME="edu.ucr.dblab.Closer"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

THE_INPUT=$1
THE_OUTPUT=$2
THE_K=$3
THE_X=328754
THE_Y=109267
THE_PARTITIONS=2000
THE_PRECISION=1000.0

spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}geospark-viz_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}proj4j-1.1.1.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS_NAME $CLASS_JAR \
    --input $THE_INPUT --output $THE_OUTPUT --partitions $THE_PARTITIONS --precision $THE_PRECISION \
    --x $THE_X --y $THE_Y --k $THE_K

