#!/bin/bash

JAR="$HOME/RIDIR/Code/SDCEL/target/scala-2.11/sdcel_2.11-0.1.jar"
LIBSPATH="$HOME/Spark/2.4/jars"
LIBS="${LIBSPATH}/geospark-1.2.0.jar:${LIBSPATH}/scallop_2.11-3.1.5.jar:${LIBSPATH}/scala-library-2.11.12.jar:${LIBSPATH}/jgrapht-core-1.4.0.jar"
CLASS="sdcel.bo.RangeTester"

THE_DIR=$1
THE_PATH="/home/and/RIDIR/Code/R/nedges/pids"
SS=(0 1 2 3 4 5 6 7 8 9)

for N in {1..5}; do
    for i in ${!SS[@]}; do
	echo "scala -cp $LIBS:$JAR $CLASS --input1 ${THE_PATH}/${THE_DIR}/A$i.wkt --input2 ${THE_PATH}/${THE_DIR}/B$i.wkt --tag1 $THE_DIR --tag2 $i --appid $N"
	scala -cp $LIBS:$JAR $CLASS --input1 ${THE_PATH}/${THE_DIR}/A$i.wkt --input2 ${THE_PATH}/${THE_DIR}/B$i.wkt --tag1 $THE_DIR --tag2 $i --appid $N
    done
done
