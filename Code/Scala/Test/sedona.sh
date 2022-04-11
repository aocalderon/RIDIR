#!/bin/bash

PACKAGES="org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2,org.apache.sedona:sedona-core-3.0_2.12:1.0.1-incubating,org.apache.sedona:sedona-sql-3.0_2.12:1.0.1-incubating"
CLASS_JAR="${HOME}/RIDIR/Code/Scala/Test/target/scala-2.11/tester_2.11-0.1.jar"
CLASS_NAME="edu.ucr.dblab.tests.TestEncoders"
LOG_FILE="${HOME}/Spark/3.2/conf/log4j.properties"


spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --packages $PACKAGES \
    --class $CLASS_NAME $CLASS_JAR
