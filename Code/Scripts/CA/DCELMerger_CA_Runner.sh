#!/bin/bash

N=$1

PARTITIONS=1024
CORES=9
EXECUTORS=12
TCORES=$((CORES * EXECUTORS))
SCRIPT="DCELMerger_CA.sh"

#ES=( 1 2 4 8 )

for i in $(seq 1 $N); do
    #for e in "${ES[@]}"; do
    for p in $(seq 1 20); do
	./${SCRIPT} -p $((p * $TCORES)) -e $EXECUTORS -c $CORES
    done
done    
