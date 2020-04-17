#!/bin/bash

CAPACITY=$1
FRACTION=$2
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties
MASTER=local[4]

A=$HOME/Datasets/WKT/CaliA.wkt
B=$HOME/Datasets/WKT/CaliB.wkt

spark-submit \
    --master $MASTER \
    --class DatasetSplitter $CLASS_JAR \
    --input1 $A --offset1 0 \
    --input2 $B --offset2 0 \
    --capacity $CAPACITY --fraction $FRACTION 
