#!/bin/bash

N=2
for n in `seq 1 $N`
do
    for sample in `seq 0 9`
    do
	tag=$((sample + 1))
	echo "python /home/acald013/RIDIR/Code/Validation/GeoPandasRunner.py --source ~/Datasets/Validation/SampleCA_source${sample}.wkt --target ~/Datasets/Validation/SampleCA_target${sample}.wkt --tag ${tag}0%"
	python /home/acald013/RIDIR/Code/Validation/GeoPandasRunner.py --source ~/Datasets/Validation/SampleCA_source${sample}.wkt --target ~/Datasets/Validation/SampleCA_target${sample}.wkt --tag ${tag}0%
    done
done
