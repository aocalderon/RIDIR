#!/bin/bash

N=$1

#PARTITIONS=1024
CORES=8
EXECUTORS=12
P=$((CORES * EXECUTORS))
SCRIPT="DCELMerger_CA.sh"

#ES=( 1 2 4 8 )

for i in $(seq 1 $N); do
    #for e in "${ES[@]}"; do
    for x in $(seq 2 2 16); do
	./${SCRIPT} -p $((x * $P)) -e $EXECUTORS -c $CORES
    done
done    
