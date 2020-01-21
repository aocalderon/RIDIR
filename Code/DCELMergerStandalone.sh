#!/bin/bash

MASTER=spark://dblab-rack15:7077
CORES=4
EXECUTORS=3
JAR=/home/acald013/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar

spark-submit \
    --master $MASTER \
    --conf spark.executor.instances=$CORES \
    --conf spark.executor.cores=$EXECUTORS \
    --class DCELMerger $JAR \
    --input1 ~/RIDIR/Datasets/Phili/phili_2000_2272.wkt --offset1 2 \
    --input2 ~/RIDIR/Datasets/Phili/phili_2010_2272.wkt --offset2 2 \
    --nlevels 8 --maxentries 100 --fraction 0.1 \
    --save --debug
