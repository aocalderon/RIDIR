#!/bin/bash

MASTER=local[10]
EXECUTORS=12
CORES=8
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/Scala/Test/target/scala-2.11/tester_2.11-0.1.jar 
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

THE_INPUT="hdfs:///user/acald013/CA/CGAL/B.wkt"
#THE_CLIP="Polygon ((-150066.4127 -605327.0480, 310002.8938 -605327.0480, 310002.8938 -77440.1680, -150066.4127 -77440.1680, -150066.4127 -605327.0480))"
THE_OUTPUT="/tmp/edgesBS.wkt"
THE_PARTITIONS=1024

spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}geospark-viz_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}proj4j-1.1.1.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class Clipper $CLASS_JAR --input $THE_INPUT --output $THE_OUTPUT --partitions $THE_PARTITIONS --precision 10000.0

