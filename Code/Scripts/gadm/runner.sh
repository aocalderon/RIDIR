#!/bin/bash

N=$1

#PARTITIONS=1024
CORES=8
EXECUTORS=6
PS=( 32 64 )
SCRIPT="DCELMerger_gadm.sh"

for i in $(seq 1 $N); do
    echo "Experiment No. $i"
    for P in ${PS[@]}; do
	echo "./${SCRIPT} -p $P -l -c $CORES"
	./${SCRIPT} -p $P -l -c $CORES
    done
done    
