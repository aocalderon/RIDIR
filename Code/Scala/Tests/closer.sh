#!/bin/bash

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
	-i|--input)
	    INPUT="$2"
	    shift
	    shift
	    ;;
	-o|--output)
	    OUTPUT="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-x)
	    X="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-y)
	    Y="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-k)
	    K="$2"
	    shift # past argument
	    shift # past value
	    ;;
	-*|--*)
	    echo "Unknown option $1"
	    exit 1
	    ;;
	*)
	    POSITIONAL_ARGS+=("$1") # save positional arg
	    shift # past argument
	    ;;
    esac
done
set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters

echo "INPUT    = $INPUT"
echo "OUTPUT   = $OUTPUT"

PACKAGES=org.apache.sedona:sedona-core-3.0_2.12:1.0.1-incubating
PACKAGES=$PACKAGES,org.apache.sedona:sedona-sql-3.0_2.12:1.0.1-incubating
PACKAGES=$PACKAGES,org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating
PACKAGES=$PACKAGES,org.datasyslab:geotools-wrapper:1.1.0-25.2
PACKAGES=$PACKAGES,org.rogach:scallop_2.12:4.1.0

CLASS_JAR=${HOME}/RIDIR/Code/Scala/Tests/target/scala-2.12/testers_2.12-0.1.jar
CLASS_NAME=edu.ucr.dblab.tests.Closer
LOG_FILE=${HOME}/Spark/3.2/conf/log4j.properties

PARAMS=(
    --files $LOG_FILE \
	    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
	    --conf spark.executor.memory=5g \
	    --packages $PACKAGES \
	    --class $CLASS_NAME $CLASS_JAR \
	    --input $INPUT --output $OUTPUT --x $X --y $Y --k $K
)
echo "${HOME}/Spark/3.2/bin/spark-submit ${PARAMS[@]}"
${HOME}/Spark/3.2/bin/spark-submit ${PARAMS[@]}
