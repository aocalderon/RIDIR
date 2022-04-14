#!/bin/bash

DATASET="OR"
PARTITIONS=464

./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 0 -n 5

./Perf -d Census/S/$DATASET -t 1e-3 -p  -m yarn -o 1 -n 5

./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 4 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 6 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 8 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 10 -n 5
