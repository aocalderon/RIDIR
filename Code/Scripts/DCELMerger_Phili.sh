#!/bin/bash

PARTITIONS=512
EXECUTORS=36
CORES=3
DMEMORY=4g
EMEMORY=4g
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=local[10]

PHILI1=/user/acald013/Datasets/Phili/phili_2000_2272.wkt
PHILI2=/user/acald013/Datasets/Phili/phili_2010_2272.wkt

spark-submit --files $LOG_FILE \
	     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
	     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
	     --master $MASTER --deploy-mode client \
	     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
	     --class DCELMerger $CLASS_JAR \
	     --input1 $PHILI1 --offset1 2 --input2 $PHILI2 --offset2 2 \
	     --partitions $PARTITIONS --save --debug

