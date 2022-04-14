#!/bin/bash

./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 5 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 6 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 7 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 11 -n 5
./Perf -d Census/S/CA -t 1e-3 -p 1432 -m yarn -o 2 -e 12 -n 5
