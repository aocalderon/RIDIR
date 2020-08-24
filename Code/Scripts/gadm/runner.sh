#!/bin/bash

N=$1

#PARTITIONS=1024
CORES=9
EXECUTORS=12
PS=( 4000 6000 8000 10000 12000 )
SCRIPT="DCELMerger_gadm.sh"

for i in $(seq 1 $N); do
    echo "Experiment No. $i"
    for P in ${PS[@]}; do
	echo "./${SCRIPT} -p $P -e $EXECUTORS -c $CORES"
	./${SCRIPT} -p $P -e $EXECUTORS -c $CORES
    done
done    
