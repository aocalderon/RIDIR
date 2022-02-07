#!/bin/bash

JAR="$HOME/RIDIR/Code/Scala/Test/target/scala-2.11/tester_2.11-0.1.jar"
LIBSPATH="$HOME/Spark/2.4/jars"
LIBS="${LIBSPATH}/geospark-1.2.0.jar"
CLASS="edu.ucr.dblab.Adder"

scala -cp $LIBS:$JAR $CLASS 
