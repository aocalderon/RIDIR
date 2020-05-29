#!/bin/bash

N=$1

PARTITIONS=1024
CORES=9
SCRIPT="DCELMerger_CA.sh"

ES=( 1 2 4 8 )

for i in $(seq 1 $N); do
    for e in "${ES[@]}"; do
	./${SCRIPT} -p $PARTITIONS -e $e -c $CORES
    done
done    
