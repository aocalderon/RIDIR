#!/bin/bash

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
	-d|--dataset)
	    DATASET="$2"
	    shift
	    shift
	    ;;
	-p|--partitions)
	    PARTITIONS="$2"
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

echo "DATASET    = $DATASET"
echo "PARTITIONS = $PARTITIONS"

./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 0 -n 1

./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 1 -n 1

./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 4 -n 1
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 5 -n 5
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 6 -n 5
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 7 -n 5
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 8 -n 5
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 9 -n 5
#./Perf3 -d Census/K/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 10 -n 5
