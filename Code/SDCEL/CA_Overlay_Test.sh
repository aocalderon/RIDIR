#!/bin/bash

./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 0 -n 5

./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 1 -n 5

./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 4 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 6 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 8 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 10 -n 5
