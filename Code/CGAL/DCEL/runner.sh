#!/bin/bash

N=$1

for i in $(seq 1 $N); do
    #./dcel -a ~/Datasets/WKT/CaliA.wkt -b ~/Datasets/WKT/CaliB.wkt -d
    ./dcel -a /home/acald013/Datasets/WKT/gadm/gadmA.wkt -b /home/acald013/Datasets/WKT/gadm/gadmB.wkt
done    
