#!/bin/bash

DATASET="TX"
PARTITIONS=1417

./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 0 -n 5

./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 1 -n 5

./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 4 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 5 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 6 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 7 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 8 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 9 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 10 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 11 -n 5
./Perf -d Census/S/$DATASET -t 1e-3 -p $PARTITIONS -m yarn -o 2 -e 12 -n 5
