#!/bin/bash

JAR="${HOME}/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar"
CLASS="edu.ucr.dblab.sdcel.DCELMerger2"

spark-submit --master local[7] --class $CLASS $JAR \
	     --input1 ~/RIDIR/Code/Tests/Iceland/edgesA.wkt \
	     --input2 ~/RIDIR/Code/Tests/Iceland/edgesB.wkt \
	     --quadtree ~/RIDIR/Code/Tests/Iceland/quadtree.wkt \
	     --boundary ~/RIDIR/Code/Tests/Iceland/boundary.wkt \
	     --local --debug
