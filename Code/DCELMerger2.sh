#!/bin/bash

PARTITIONS=( 2250 2500 2750 3000 )
for N in {1..5}; do
    for P in ${PARTITIONS[@]}; do
	./DCELMerger.sh $P
    done
done
	 
	 
	 
