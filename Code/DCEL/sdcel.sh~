#!/bin/bash

JAR="$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar"
CLASS="edu.ucr.dblab.sdcel.DCELPartitioner2"

spark-submit \
    --master local[7] \
    --class $CLASS $JAR \
    --input1 $1 \
    --input2 $2 \
    --partitions $3 --local
