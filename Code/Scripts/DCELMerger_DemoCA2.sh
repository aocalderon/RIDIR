#!/bin/bash

PARTITIONS=4
EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=local[4]

A=/user/acald013/Datasets/CA/E2000.tsv
B=/user/acald013/Datasets/CA/E2010.tsv

spark-submit --files $LOG_FILE \
	     --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
	     --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
	     --master $MASTER --deploy-mode client \
	     --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
	     --class DCELMerger $CLASS_JAR \
	     --input1 $A --offset1 3 --input2 $B --offset2 3 \
	     --nlevels 2 --maxentries 100 --fraction 1 --custom \
	     --save --debug

