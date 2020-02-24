#!/bin/bash

PARTITIONS=1024
EXECUTORS=36
CORES=3
DMEMORY=4g
EMEMORY=5g
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=yarn

A=/user/acald013/Datasets/CA/cali2000_polygons6414.tsv
B=/user/acald013/Datasets/CA/cali2010_polygons6414.tsv

spark-submit --files $LOG_FILE \
	     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
	     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
	     --master $MASTER --deploy-mode client \
	     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
	     --class DCELMerger $CLASS_JAR \
	     --input1 $A --offset1 2 --input2 $B --offset2 2 \
	     --partitions $PARTITIONS --save --debug

