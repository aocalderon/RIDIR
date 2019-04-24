#!/bin/bash

N=4
for n in `seq 1 $N`
do
    for sample in `seq 0 9`
    do
	echo "spark-submit /home/acald013/RIDIR/Code/Areal/target/scala-2.11/areal_2.11-0.1.jar --source ~/Datasets/Validation/SampleCA_source${sample}.wkt --target ~/Datasets/Validation/SampleCA_target${sample}.wkt"
	spark-submit /home/acald013/RIDIR/Code/Areal/target/scala-2.11/areal_2.11-0.1.jar --source ~/Datasets/Validation/SampleCA_source${sample}.wkt --target ~/Datasets/Validation/SampleCA_target${sample}.wkt
    done
done
