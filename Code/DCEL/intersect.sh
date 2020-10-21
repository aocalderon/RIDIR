#!/bin/bash

JAR="$HOME/RIDIR/Code/DCEL/target/scala-2.11/dcel_2.11-0.1.jar"
LIBSPATH="$HOME/Spark/2.4/jars"
LIBS="${LIBSPATH}/geospark-1.2.0.jar"
CLASS="edu.ucr.dblab.sdcel.SweepLine3"

scala -cp $LIBS:$JAR $CLASS
