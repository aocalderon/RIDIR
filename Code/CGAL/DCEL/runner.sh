#!/bin/bash

N=$1

for i in $(seq 1 $N); do
    ./dcel -a ~/Datasets/WKT/CaliA.wkt -b ~/Datasets/WKT/CaliB.wkt
done    
